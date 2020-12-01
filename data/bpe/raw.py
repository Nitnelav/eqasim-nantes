import simpledbf
from tqdm import tqdm
import pandas as pd
import os

"""
This stage loads the raw data from the French service registry.
"""

def configure(context):
    context.config("data_path")
    context.config("bpe_path", "bpe_2015/bpe15_ensemble_xy.dta")

    context.stage("data.spatial.codes")

COLUMNS = [
    "dciris", "lambert_x", "lambert_y",
    "typequ", "depcom"
]

def execute(context):
    df_codes = context.stage("data.spatial.codes")
    requested_departements = df_codes["departement_id"].unique()
    requested_communes = df_codes["commune_id"].unique()

    table_iter = pd.read_stata("%s/%s" % (context.config("data_path"), context.config("bpe_path")), chunksize=10240)
    df_records = []

    with context.progress( label = "Reading enterprise census ...") as progress:
        for df_chunk in table_iter:
            progress.update(len(df_chunk))

            df_chunk = df_chunk[df_chunk["depcom"].isin(requested_communes)]
            df_chunk = df_chunk[COLUMNS]

            if len(df_chunk) > 0:
                df_records.append(df_chunk)

    return pd.concat(df_records)

def validate(context):
    if not os.path.exists("%s/%s" % (context.config("data_path"), context.config("bpe_path"))):
        raise RuntimeError("BPE 2015 data is not available")

    return os.path.getsize("%s/%s" % (context.config("data_path"), context.config("bpe_path")))
