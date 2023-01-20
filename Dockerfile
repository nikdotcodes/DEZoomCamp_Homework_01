FROM python:3.11

RUN apt install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_csv_to_postgres.py pipeline.py

ENTRYPOINT ["python", "pipeline.py"]