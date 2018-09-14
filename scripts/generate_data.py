import sys
from textwrap import dedent
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from contextlib import contextmanager

import pyodbc
import pandas
import numpy

import wqio

OUTDIR = Path('../data')

NSQDQUERY = dedent("""\
    SELECT
        Stations.Station_Code,
        Stations.Station_Name,
        Stations.EPA_Rain_Zone,
        Stations.State,
        Stations.[Principal Landuses],
        Events.Start_Date,
        Events.End_Date,
        Events.[Days since last rain],
        Events.[Precipitation_Depth_(in)],
        (Events.[3h_or_Total_Event]) as sample_event_type,
        Events.Type_Sampler,
        Results.EMC_Calculation,
        Results.fraction,
        Results.parameter,
        Results.qual,
        Results.res,
        Results.units
    FROM Stations
    INNER JOIN (
        Events
        INNER JOIN Results ON Events.Event_Code = Results.Event_Code
    ) ON Stations.Station_Code = Events.Station_Code;
""")

_LU_MAP = {
    'ID': 'industrial',
    'RE': 'residential',
    'CO': 'commercial',
    'OP': 'open space',
    'CO_MIX': 'commercial',
    'ID_MIX': 'industrial',
    'FW_MIX': 'freeway',
    'RE_MIX': 'residential',
    'OP_MIX': 'open space',
    'IS': 'institutional',
    'UNK': 'unknown',
    'FW': 'freeway',
    'IS_MIX': 'institutional',
    'UNK_MIX': 'unknown',
}

_EQUIP_MAP = {
    'MA': 'manual',
    'AU': 'automatic'
}

BMPQUERY = dedent("""\
    SELECT
        [tblBMPCODES].[CAT_2016] as [bmpcat],
        [BMP INFO S02].[TBMPT 2009] as [bmptype],
        [BMP INFO S02].[Analysis_Category] AS [category],
        [WATER QUALITY].[SITEID] as [site_id],
        [TESTSITE A01].[SITENAME] as [site],
        [BMP INFO S02].[PDF ID] as [pdf_id],
        [MONITORING STATIONS].[BMPID] as [bmp_id],
        [MONITORING STATIONS].[BMPName] as [bmp],
        [WATER QUALITY].[MSID] as [ms_id],
        [WATER QUALITY].[MSNAME] as [ms],
        [WATER QUALITY].[Storm #] as [storm],
        [WATER QUALITY].[SAMPLEDATE] as [sampledate],
        [WATER QUALITY].[SAMPLETIME] as [sampletime],
        [tblAnalysisGroups].[Group] as [paramgroup],
        [tblAnalysisGroups].[Common Name] as [parameter],
        [WATER QUALITY].[WQX Parameter] as [raw_parameter],
        [WATER QUALITY].[Sample Fraction] as [fraction],
        [WATER QUALITY].[MEDIA] as [media],
        [WATER QUALITY].[WQ Analysis Value] as [wq_value],
        [WATER QUALITY].[WQ UNITS] as [wq_units],
        [WATER QUALITY].[QUAL] as [wq_qual],
        [MONITORING STATIONS].[Monitoring Station Type] as [station],
        [WATER QUALITY].[SGTCODE],
        [WATER QUALITY].[SGTCodeDescp] as [watertype],
        [WATER QUALITY].[STCODE],
        [WATER QUALITY].[STCODEDescp] as [sampletype],
        [WATER QUALITY].[AFPA] as [initialscreen],
        [MONITORING STATIONS].[Use in BMP WQ Analysis] AS [wqscreen],
        [MONITORING STATIONS].[Use in BMP Category Analysis] AS [catscreen],
        [WATER QUALITY].[Cat_AnalysisFlag] AS [balanced],
        [WATER QUALITY].[COMMENT] as [comment],
        [EVENT].[Event Type] as [event_type],
        [TESTSITE A01].[EPA Rain Zone] as [epazone],
        [TESTSITE A01].[State] as [state],
        [TESTSITE A01].[Country] as [country],
        [tblAnalysisGroups].[WQID] as [wq_id],
        [WATER QUALITY].[DL] as [DL],
        [WATER QUALITY].[DLT] as [dl_type],
        [WATER QUALITY].DLUnits as [dl_units],
        [MONITORING STATIONS].[Watershed ID] as [ws_id]
    FROM
        [TESTSITE A01] INNER JOIN (
            (
                [EVENT] RIGHT JOIN (
                    (
                        (
                            [WATER QUALITY] LEFT JOIN [MONITORING STATIONS] ON [WATER QUALITY].[MSID] = [MONITORING STATIONS].[Monitoring Station ID]
                        ) LEFT JOIN [BMP INFO S02] ON [MONITORING STATIONS].[BMPID] = [BMP INFO S02].[BMPID]
                    ) LEFT JOIN [tblBMPCODES] ON [BMP INFO S02].[TBMPT 2009] = [tblBMPCODES].[TBMPT]
                ) ON ([EVENT].[NSWID] = [WATER QUALITY].[SITEID]) AND ([EVENT].[Storm Event] = [WATER QUALITY].[Storm #])
            ) LEFT JOIN [tblAnalysisGroups] ON
                ([WATER QUALITY].[WQX Parameter] = [tblAnalysisGroups].[WQX Parameter]) AND
                ([WATER QUALITY].[Sample Fraction] = [tblAnalysisGroups].[Analysis Sample Fraction])
        ) ON [TESTSITE A01].[NSWID] = [WATER QUALITY].[SITEID]
    WHERE (
        (
            [WATER QUALITY].[MEDIA] = 'water'
        ) AND (
            ([MONITORING STATIONS].[Monitoring Station Type]) <> 'rain gauge'
        ) AND (
            ([WATER QUALITY].[SGTCODE]) = 1
        ) AND (
            ([WATER QUALITY].[AFPA]) in ('Yes', 'inc')
        ) AND (
            ([MONITORING STATIONS].[Use in BMP WQ Analysis]) = 'Yes'
        ) AND (([EVENT].[Event Type]) <> 'baseflow')
    )
    ORDER BY
        [TESTSITE A01].[SITENAME],
        [MONITORING STATIONS].[BMPName],
        [WATER QUALITY].[Storm #],
        [WATER QUALITY].[SAMPLEDATE],
        [WATER QUALITY].[WQX Parameter],
        [WATER QUALITY].[Sample Fraction],
        [MONITORING STATIONS].[Monitoring Station Type];
""")


def setup_parameters(df):
    has_dissolved = df.loc[
        df.loc[:, 'parameter_fraction'].str.lower() == 'dissolved',
        'parameter_family'
    ].str.lower().str.strip().unique()
    return df.assign(
        parameter=numpy.where(
            df['parameter_family'].str.lower().str.strip().isin(has_dissolved),
            df['parameter_family'].str.strip() + ', ' + df['parameter_fraction'],
            df['parameter_family']
        )
    )


def convert_dates(df):
    return df.assign(
        start_date=pandas.to_datetime(df['start_date']),
        end_date=pandas.to_datetime(df['end_date']),
    )


@contextmanager
def accdb_connection(dbfile):
    _driver = r'{Microsoft Access Driver (*.mdb, *.accdb)}'
    connection_string = f'Driver={_driver};DBQ={dbfile}'
    cnn = pyodbc.connect(connection_string)
    yield cnn
    cnn.close()


def dump_to_zip(df, name, keep_csv=False):
    outcsv = OUTDIR / f'{name}.csv'
    outzip = OUTDIR / f'{name}.zip'

    df.to_csv(outcsv, index=False, encoding='utf-8')

    with ZipFile(outzip, mode='w', compression=ZIP_DEFLATED) as z:
        z.write(outcsv, arcname=outcsv.name)

    if not keep_csv:
        outcsv.unlink()

    return outzip


def make_nsqd(dbfile):
    with accdb_connection(Path(dbfile)) as cnn:
        res = (
            pandas.read_sql(NSQDQUERY, cnn)
                .rename(columns=lambda c: c.lower().replace(' ', '_'))
                .rename(columns={
                    'parameter': 'parameter_family',
                    'fraction': 'parameter_fraction',
                    'station_name': 'site',
                    'epa_rain_zone': 'rain_zone',
                    'principal_landuses': 'landuse_orig',
                    'precipitation_depth_(in)': 'precip_depth_in',
                    'emc_calculation': 'sampletype',
                    'res': 'value',
                })
                .pipe(setup_parameters)
                .pipe(convert_dates)
                .assign(season=lambda df: df['start_date'].map(wqio.utils.getSeason))
                .assign(landuse_primary=lambda df: df['landuse_orig'].map(_LU_MAP))
                .assign(sample_equipment=lambda df: df['type_sampler'].map(_EQUIP_MAP))
                .assign(sampletype='composite')
                .drop(columns=['station_code'])
        )


    return res


def make_bmpdb(dbfile):
    with accdb_connection(dbfile=dbfile) as cnn:
        res = (
            pandas.read_sql(BMPQUERY, cnn)
                .loc[lambda df: df.loc[:, 'parameter'].str.strip() != 'Particle Concentration', :]
        )

    return res


if __name__ == '__main__':
    keep_csv = (len(sys.argv) > 1) and sys.argv[1] in ('-k', '--keep')
    bmpfile = Path(r"P:\Reference\Data\BMP_Database\201808\Master BMP Database v 08-22-2018 - Web.accdb")
    nsqdfile = Path(r"P:\Reference\Data\Bob Pitt NSWQ DB\nsqd.accdb")

    bmpdb = make_bmpdb(bmpfile)
    nsqd = make_nsqd(nsqdfile)

    dump_to_zip(bmpdb, 'bmpdata', keep_csv=keep_csv)
    dump_to_zip(nsqd, 'nsqd', keep_csv=keep_csv)
