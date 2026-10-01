import io
import logging
import os

import geopandas
import pandas as pd
import pendulum
import requests
from airflow.sdk import ObjectStoragePath, dag, task
from shapely import wkt


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=['electric_vehicle', 'project', 'ETL']
)
def electric_vehicle_population():

    @task()
    def extract():
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        URL = "https://data.wa.gov/resource/f6w7-q2d2.csv"

        LIMIT = 100_000
        OFFSET = 0

        chunks = []
        dtype = {
            'vin_1_10': 'string',
            'county': 'string',
            'city': 'string',
            'state': 'string',
            'zip_code': 'string',
            'model_year': 'int32',
            'make': 'string',
            'model': 'string',
            'ev_type': 'string',
            'cafv_type': 'string',
            'electric_range': 'float32',
            'legislative_district': 'string',
            'dol_vehicle_id': 'string', 
            'geocoded_column': 'string', 
            'electric_utility': 'string',
            '_2020_census_tract': 'string'
        }

        logger.info('Starting data load from https://data.wa.gov/resource/f6w7-q2d2.csv')
        with requests.Session() as session:
            while True:
                response = session.get(
                    URL,
                    params={
                        '$limit': LIMIT,
                        '$offset': OFFSET,
                    },
                    timeout=600,
                )
                logger.debug(response.raise_for_status())

                chunk = pd.read_csv(io.BytesIO(response.content), dtype=dtype)

                if chunk.empty:
                    logger.info('No more rows to download.')
                    break

                chunks.append(chunk)

                OFFSET += len(chunk)

                logger.info(f'Retrieved {OFFSET:,} rows')

                if len(chunk) < LIMIT:
                    logger.info('No more rows to download.')
                    break

        df = pd.concat(chunks, ignore_index=True)

        logger.info('Data load success')

        base = ObjectStoragePath('gs://ev_population_data_pipeline_bucket/')
        raw_path = base / f'raw/{pendulum.now().year}-{pendulum.now().strftime('%b')} ev_population_raw_data.parquet'

        with raw_path.open('wb') as file:
            df.to_parquet(file)

        return raw_path

    @task()
    def transform(raw_path: ObjectStoragePath):
        categories = {
            'cafv_type': {
                'clean alternative fuel vehicle eligible',
                'eligibility unknown as battery range has not been researched',
                'not eligible due to low battery range', 
                'not defined'
            },
            'ev_type': {
                'battery electric vehicle (bev)', 
                'plug-in hybrid electric vehicle (phev)',
                'not defined'
            }}
        
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        with raw_path.open("rb") as f:
            df = pd.read_parquet(f)

        df = df.apply(lambda x: x.str.lower().str.strip() if x.dtype.name == 'string' else x)
        logger.info('Normalize string success')

        for key, val in categories.items():
            diff = set(df[key].unique()) - val
            
            df.loc[df[key].isin(diff), key] = 'not defined'

            df[key] = df[key].astype('category')
            logger.info(f'Convert {key} to category data type success')

        df.geocoded_column = geopandas.GeoSeries.from_wkt(df.geocoded_column).to_wkb()
        logger.info('Convert geocoded_column to wkb success')

        logger.info(f'Null percentage in the data: {((df.isnull().any(axis=1).sum() / len(df)) * 100.0).round(2)}%')

        null = df.isnull().any(axis=1)

        base = ObjectStoragePath('gs://ev_population_data_pipeline_bucket/')
        null_output_path = base / f'transformed/{pendulum.now().year}-{pendulum.now().strftime('%b')} ev_population_null_data.parquet'
        clean_output_path = base / f'transformed/{pendulum.now().year}-{pendulum.now().strftime('%b')} ev_population_clean_data.parquet'

        with null_output_path.open('wb') as file:
            df[null].to_parquet(file)

        logger.info(f'Successfully upload {null_output_path.name} to Google cloud storage')

        with clean_output_path.open('wb') as file:
            df[~null].to_parquet(file)

        logger.info(f'Successfully upload {clean_output_path.name} to Google cloud storage')
        logger.info('Data transformation success')

        return [null_output_path, clean_output_path]

    # @task()
    # def load(total_ev_population: int):
    #     """
    #     #### Load task
    #     Prints out the total electric vehicle population.
    #     """
    #     print(f"Total electric vehicle population is: {total_ev_population}")

    # ev_data = extract()

    # ev_summary = transform(ev_data)
    # load(ev_summary["total_ev_population"])

    raw_path = extract()
    paths = transform(raw_path)

evp = electric_vehicle_population()

if __name__ == "__main__":
    evp.test()