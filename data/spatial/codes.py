import numpy as np
import pandas as pd
import os

"""
This stages loads a file containing all spatial codes in France and how
they can be translated into each other. These are mainly IRIS, commune,
departement and région.
"""

YEAR = 2015
SOURCE = "codes_%d/reference_IRIS_geo%d.xls" % (YEAR, YEAR)

def configure(context):
    context.config("data_path")

    context.config("regions", [52])
    context.config("departments", [44])
    # context.config("comunes", [44109, 44162, 44020, 44143, 44215, 44190, 44009, 44172, 44026, 44035, 44114])
    # for tests
    # context.config("comunes", [44109])

def execute(context):
    # Load IRIS registry
    df_codes = pd.read_excel(
        "%s/%s" % (context.config("data_path"), SOURCE),
        skiprows = 5, sheet_name = "Emboitements_IRIS"
    )[["CODE_IRIS", "DEPCOM", "DEP", "REG"]].rename(columns = {
        "CODE_IRIS": "iris_id",
        "DEPCOM": "commune_id",
        "DEP": "departement_id",
        "REG": "region_id"
    })

    df_codes["iris_id"] = df_codes["iris_id"].astype("category")
    df_codes["commune_id"] = df_codes["commune_id"].astype("category")
    df_codes["departement_id"] = df_codes["departement_id"].astype("category")
    df_codes["region_id"] = df_codes["region_id"].astype(int)

    # Filter zones
    requested_regions = list(map(int, context.config("regions")))
    requested_departments = list(map(str, context.config("departments")))
    # requested_comunes = list(map(str, context.config("comunes")))

    if len(requested_regions) > 0:
        df_codes = df_codes[df_codes["region_id"].isin(requested_regions)]

    if len(requested_departments) > 0:
        df_codes = df_codes[df_codes["departement_id"].isin(requested_departments)]

    # if len(requested_comunes) > 0:
    #     df_codes = df_codes[df_codes["commune_id"].isin(requested_comunes)]

    df_codes["iris_id"] = df_codes["iris_id"].cat.remove_unused_categories()
    df_codes["commune_id"] = df_codes["commune_id"].cat.remove_unused_categories()
    df_codes["departement_id"] = df_codes["departement_id"].cat.remove_unused_categories()

    return df_codes

def validate(context):
    if not os.path.exists("%s/%s" % (context.config("data_path"), SOURCE)):
        raise RuntimeError("Spatial reference codes are not available")

    return os.path.getsize("%s/%s" % (context.config("data_path"), SOURCE))
