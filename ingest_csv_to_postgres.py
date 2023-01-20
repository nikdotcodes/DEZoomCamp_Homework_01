#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    tbl = params.tbl
    url = params.url

    csv_name = "output.csv"
    schema_name = "raw"

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    taxi_data_head = pd.read_csv(csv_name, nrows=0, compression='gzip')
    taxi_data_head.to_sql(name=tbl, schema=schema_name, con=engine, if_exists="replace")

    with pd.read_csv(csv_name, iterator=True, chunksize=50000, compression='gzip') as reader:
        for i, chunk in enumerate(reader):
            chunk.to_sql(name=tbl, schema=schema_name, con=engine, if_exists="append")
            print(f"Loaded chunk #{i}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Ingest .csv data to Postgres")

    parser.add_argument("--user", help="user name for Postgres")
    parser.add_argument("--password", help="password for user account on Postgres")
    parser.add_argument("--host", help="hostname for Postgres server")
    parser.add_argument("--port", help="port for Postgres server")
    parser.add_argument("--db", help="Postgres database name")
    parser.add_argument("--tbl", help="table to load to")
    parser.add_argument("--url", help="url of csv file to load")

    args = parser.parse_args()

    main(args)
