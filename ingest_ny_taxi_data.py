#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://nik:nik@localhost:5432/ny_taxi')
engine.connect()

taxi_data_head = pd.read_csv('data_in/green_tripdata_2019-01.csv', nrows=0)
taxi_data_head.to_sql(name='green_tripdata', con=engine, if_exists='replace')

taxi_lookup = pd.read_csv("data_in/taxi+_zone_lookup.csv")

with pd.read_csv('data_in/green_tripdata_2019-01.csv', iterator=True, chunksize=50000) as reader:
    for i, chunk in enumerate(reader):
        chunk['lpep_pickup_datetime'] = pd.to_datetime(chunk['lpep_pickup_datetime'])
        chunk['lpep_dropoff_datetime'] = pd.to_datetime(chunk['lpep_dropoff_datetime'])

        chunk.to_sql(name='green_tripdata', con=engine, if_exists='append')

        print(f"Loaded chunk #{i}")

taxi_lookup.to_sql(name="taxi_zone_lookup", con=engine, if_exists="replace")
