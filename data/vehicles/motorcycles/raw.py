import numpy as np
import pandas as pd

"""
This stage loads the raw data of 2rm 2012 survey
https://www.statistiques.developpement-durable.gouv.fr/sites/default/files/2018-11/2rm-detail-diffusion.csv
"""

def configure(context):
    context.config("data_path")

def execute(context):
    df_motorcycles = pd.read_csv("%s/2rm/2rm-detail-diffusion.csv" % context.config("data_path"), sep=";", encoding="cp1252")

    # some filters
    df_motorcycles = df_motorcycles[df_motorcycles["KMANNUEL"] > 1] # vehicle never used
    df_motorcycles = df_motorcycles[df_motorcycles["ENDURO"].astype(int) != 1] # vehicle "tout-terrain"
    df_motorcycles = df_motorcycles[df_motorcycles["AGEVEHICULE"].astype(int) < 30] # vehicle too old

    bins = [14,18,25,35,50,70,110]
    labels = ["%d_%d" % (a,b) for a,b in zip(bins, bins[1:])]
    age_cat = pd.cut(df_motorcycles["AGECONDUCTEUR"], bins=bins, labels=labels, right=False)
    # age_cat = age_cat.map({'14_18': 0, '18_25': 1, '25_35': 2, '35_50': 3, '50_70': 4, '70_110': 5})
    df_motorcycles["AGECAT"] = age_cat

    # TODO : think about the attributes we would like to groupby on
    # possible candidates are : ["AGECAT", "SEXE", "NBVOITURES", "NBVELOS", "STATUT", "PROFESSION", "DIPLOME", "NBFOYER", "NBENFANTS", "REGION", "ZONAGE", "REVENU"]
    columns_persons = ["AGECAT", "SEXE"]
    columns_vehicles = ["PF", "AGEVEHICULE", "MOTOR", "MOTEUR"]
    df_motorcycles = df_motorcycles[columns_persons + columns_vehicles + ["POIDSVEHICULE", "POIDSCONDUCTEUR"]]

    df_counts = df_motorcycles.groupby(columns_persons + columns_vehicles, observed=True)["POIDSVEHICULE"].sum().reset_index()
    return df_counts