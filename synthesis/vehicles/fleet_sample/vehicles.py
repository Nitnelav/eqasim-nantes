import re
import pandas as pd
import numpy as np
from datetime import date

"""
Creates the synthetic vehicle fleet
"""

def configure(context):
    context.stage("data.vehicles.types")
    context.stage("synthesis.vehicles.fleet_sample.cars")
    context.stage("synthesis.vehicles.fleet_sample.motorcycles")

def execute(context):

    df_vehicle_types = context.stage("data.vehicles.types")
    df_cars = context.stage("synthesis.vehicles.fleet_sample.cars")
    df_motorcycles = context.stage("synthesis.vehicles.fleet_sample.motorcycles")

    df_vehicles = pd.concat([df_cars, df_motorcycles], ignore_index=True)
    return df_vehicle_types, df_vehicles