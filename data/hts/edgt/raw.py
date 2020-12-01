from pandas.core.frame import DataFrame
import numpy as np
import pandas as pd
import os

"""
This stage loads the raw data of the specified HTS (EDGT).
"""

MENAGE_DATA_WIDTH = [1, 6, 6, 4, 1, 1, 1, 1, 1, 4, 2, 1, 4, 2, 1, 4, 2, 1, 4, 2, 1, 2, 4, 2, 2, 5, 1, 8, 1]
MENAGES_COLUMNS = [
    "MP1", "ANNE", "ZONE", "ECH", "M1", "M2", "M3D", "M5",
    "M7A1", "M13A", "M14A", "M7B1", "M13B", "M14B", "M7C1", "M13C", "M14C", "M7D1", "M13D", "M14D",
    "M6", "M7", "MLA", "MLB1", "MLB2", "MLC", "MLD", "COEM", "MFIN"
]
KEEP_MENAGES_COLUMNS = [
    "ZONE", "ECH", "M5", "M6", "M7", "COEM"
]
MENAGE_DTYPE = {
    "ZONE": 'string',
    "ECH": 'string',
    "M5": 'Int8',
    "M6": 'Int8',
    "M7": 'Int8',
    "COEM": 'Float32'
}

PERSO_DATA_WIDTH = [
    1, 6, 4, 2, 1, 1,
    1, 1, 1, 2, 1, 1, 1, 1, 1,
    6, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1,
    6, 8, 8, 1]
PERSONNES_COLUMNS = [
    "PP1", "ZONE", "ECH", "PID", "PENQ", "PP3",
    "P1", "P2", "P3", "P4", "P5", "P6", "P7", "P9", "P12",
    "P13A", "P15", "P17", "P17A", "P23A", "P20", "P21", "P23", "P24", "P25",
    "P19", "P19A", "PL27", "PL29", "PL30",
    "DP13", "COEP", "COEQ", "PFIN"
]
KEEP_PERSONNES_COLUMNS = [
    "ZONE", "ECH", "PID", "PP3", "P1", "P2", "P4", "P5", "P6", "P7", "P9", "COEP", "COEQ"
]
PERS_DTYPE = {
    "ZONE": 'string',
    "ECH": 'string',
    "PID": 'Int8',
    "PP3": 'Int32',
    "P1": 'category',
    "P2": 'Int8',
    "P4": 'Int8',
    "P5": 'category',
    "P6": 'category',
    "P7": 'category',
    "P9": 'Int8',
    "COEP": 'string',
    "COEQ": 'string'
}

DEPLA_DATA_WIDTH = [1, 6, 4, 2, 2, 2, 2, 6, 2, 2, 2, 2, 2, 6, 2, 2, 3, 1, 2, 2, 8, 8, 8, 1]
DEPLACEMENTS_COLUMNS = [
    "DP1", "ZONE", "ECH", "PID", "DID",
    "D2A", "D2B", "D3", "D4A", "D4B", "D5A", "D5B", "D6", "D7", "D8A", "D8B", "D8C", "D9",
    "MODP", "MOIP", "DOIB", "DIST", "DISP", "DFIN"
]
KEEP_DEPLACEMENTS_COLUMNS = [
    "ZONE", "ECH", "PID", "DID",
    "D2A", "D2B", "D3", "D4A", "D4B", "D5A", "D5B", "D6", "D7", "D8A", "D8B", "D8C", "D9",
    "MODP", "MOIP", "DOIB", "DIST", "DISP"
]
DEPLA_DTYPE = {
    "ZONE": 'string',
    "ECH": 'string',
    "PID": 'Int8',
    "DID": 'Int8',
    "D2A": 'Int8',
    "D2B": 'category',
    "D3": 'string',
    "D4A": 'Int8',
    "D4B": 'Int8',
    "D5A": 'Int8',
    "D5B": 'category',
    "D6": 'Int16',
    "D7": 'string',
    "D8A": 'Int8',
    "D8B": 'Int8',
    "D8C": 'Int16',
    "D9": 'Int8',
    "MODP": 'Int8',
    "MOIP": 'Int8',
    "DOIB": 'Float32',
    "DIST": 'Float32',
    "DISP": 'Float32'
}

TRAJET_DATA_WIDTH = [1, 6, 4, 2, 2, 1, 2, 2, 6, 6, 2, 1, 1, 1, 1, 1, 2, 8, 8, 8, 8, 8, 1]
TRAJETS_COLUMNS = [
    "TP1", "ZONE", "ECH", "PID", "DID", "TID",
    "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T8B", "T9", "T10", "T11",
    "TOIS", "TDIS", "TDIP", "TDMO", "TDMD", "TFIN"
]
KEEP_TRAJETS_COLUMNS = [
    "ZONE", "ECH", "PID", "DID", "TID"
]
TRAJ_DTYPE = {
    "ZONE": 'string',
    "ECH": 'string',
    "PID": 'Int8',
    "DID": 'Int8',
    "TID": 'Int8'
}

def configure(context):
    context.config("data_path")

def execute(context):
    df_menages = pd.read_fwf(
        "%s/edgt_2015/EDGT_44_MENAGE.txt" % context.config("data_path"),
        widths=MENAGE_DATA_WIDTH, header=None,
        names=MENAGES_COLUMNS, usecols=KEEP_MENAGES_COLUMNS, dtype=MENAGE_DTYPE
    )
    df_menages["MID"] = df_menages["ZONE"].str.cat(df_menages["ECH"]).astype(np.int)
    df_menages.info(verbose=False, memory_usage="deep")

    df_personnes = pd.read_fwf(
        "%s/edgt_2015/EDGT_44_PERSO.txt" % context.config("data_path"),
        widths=PERSO_DATA_WIDTH, header=None, names=PERSONNES_COLUMNS, usecols=KEEP_PERSONNES_COLUMNS, dtype=PERS_DTYPE
    )
    df_personnes["MID"] = df_personnes["ZONE"].str.cat(df_personnes["ECH"]).astype(np.int)
    df_personnes.info(verbose=False, memory_usage="deep")

    df_deplacements = pd.read_fwf(
        "%s/edgt_2015/EDGT_44_DEPLA.txt" % context.config("data_path"),
        widths=DEPLA_DATA_WIDTH, header=None, names=DEPLACEMENTS_COLUMNS, usecols=KEEP_DEPLACEMENTS_COLUMNS, dtype=DEPLA_DTYPE
    )
    df_deplacements["MID"] = df_deplacements["ZONE"].str.cat(df_deplacements["ECH"]).astype(np.int)
    df_deplacements.info(verbose=False, memory_usage="deep")

    df_trajets = pd.read_fwf(
        "%s/edgt_2015/EDGT_44_TRAJET.txt" % context.config("data_path"),
        widths=TRAJET_DATA_WIDTH, header=None, names=TRAJETS_COLUMNS, usecols=KEEP_TRAJETS_COLUMNS, dtype=TRAJ_DTYPE
    )
    df_trajets["MID"] = df_trajets["ZONE"].str.cat(df_trajets["ECH"]).astype(np.int)
    df_trajets.info(verbose=False, memory_usage="deep")

    return df_menages, df_personnes, df_deplacements, df_trajets

def validate(context):
    for name in ("EDGT_44_PERSO.txt", "EDGT_44_PERSO.txt", "EDGT_44_DEPLA.txt", "EDGT_44_TRAJET.txt"):
        if not os.path.exists("%s/edgt_2015/%s" % (context.config("data_path"), name)):
            raise RuntimeError("File missing from EDGT: %s" % name)

    return [
        os.path.getsize("%s/edgt_2015/EDGT_44_PERSO.txt" % context.config("data_path")),
        os.path.getsize("%s/edgt_2015/EDGT_44_PERSO.txt" % context.config("data_path")),
        os.path.getsize("%s/edgt_2015/EDGT_44_DEPLA.txt" % context.config("data_path")),
        os.path.getsize("%s/edgt_2015/EDGT_44_TRAJET.txt" % context.config("data_path"))
    ]
