from airflow.sdk import dag, task
import pendulum

import requests
import pandas as pd
import io

@dag(
    schedule=None,
    start_date=pendulum.now("UTC"),
    tags=['electric_vehicle', 'project', 'ETL'],
)
def electric_vehicle_population():
    @task(show_return_value_in_logs=False)
    def extract():

        URL = "https://data.wa.gov/resource/f6w7-q2d2.csv"

        LIMIT = 100_000
        OFFSET = 0

        chunks = []
    
        with requests.Session() as session:
            while True:
                response = session.get(
                    URL,
                    params={
                        "$limit": LIMIT,
                        "$offset": OFFSET,
                    },
                    timeout=600,
                )
                response.raise_for_status()
                print('Response status code:', response.status_code)
                
                chunk = pd.read_csv(io.BytesIO(response.content))
                print('After reading the chunk, the shape is:', chunk.shape)

                if chunk.empty:
                    print("No more rows to download.")
                    break

                chunks.append(chunk)

                OFFSET += len(chunk)

                print(f"Retrieved {OFFSET:,} rows")

                if len(chunk) < LIMIT:
                    print("No more rows to download.")
                    break

        df = pd.concat(chunks, ignore_index=True)
        return df

    # @task(multiple_outputs=True)
    # def transform(ev_data_dict: dict):
    #     """
    #     #### Transform task
    #     Computes the total electric vehicle population over the years.
    #     """
    #     total_ev_population = sum(ev_data_dict.values())
    #     return {"total_ev_population": total_ev_population}

    # @task()
    # def load(total_ev_population: int):
    #     """
    #     #### Load task
    #     Prints out the total electric vehicle population.
    #     """
    #     print(f"Total electric vehicle population is: {total_ev_population}")

    ev_data = extract()

    # ev_summary = transform(ev_data)
    # load(ev_summary["total_ev_population"])

electric_vehicle_population()