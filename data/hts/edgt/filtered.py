import data.hts.hts as hts

"""
This stage filters out EDGT observations
"""

def configure(context):
    context.stage("data.hts.edgt.cleaned")
    context.stage("data.spatial.codes")

def execute(context):
    df_codes = context.stage("data.spatial.codes")

    df_households, df_persons, df_trips = context.stage("data.hts.edgt.cleaned")

    # Filter for non-residents
    requested_departments = df_codes["departement_id"].unique()
    f = df_persons["departement_id"].astype(str).isin(requested_departments) # pandas bug!
    df_persons = df_persons[f]

    # Filter for people going outside of the area (because they have NaN distances)
    remove_ids = set()

    remove_ids |= set(df_trips[
        ~df_trips["origin_departement_id"].astype(str).isin(requested_departments) | ~df_trips["destination_departement_id"].astype(str).isin(requested_departments)
    ]["person_id"].unique())

    remove_ids |= set(df_persons[
        ~df_persons["departement_id"].isin(requested_departments)
    ])

    df_persons = df_persons[~df_persons["person_id"].isin(remove_ids)]

    # Only keep trips and households that still have a person
    df_trips = df_trips[df_trips["person_id"].isin(df_persons["person_id"].unique())]
    df_households = df_households[df_households["household_id"].isin(df_persons["household_id"])]

    # Finish up
    df_households = df_households[hts.HOUSEHOLD_COLUMNS + ["MID"]]
    df_persons = df_persons[hts.PERSON_COLUMNS + ["MID", "PID"]]
    df_trips = df_trips[hts.TRIP_COLUMNS + ["euclidean_distance"] + ["MID", "PID", "DID"]]

    hts.check(df_households, df_persons, df_trips)

    df_trips.to_csv("/home/valoo/reference_edgt.csv", sep=";")

    return df_households, df_persons, df_trips
