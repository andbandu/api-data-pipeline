import requests 
import pandas as pd
import pandera.pandas as pa
from datetime import datetime, timezone 
import os  
from prefect import flow, task    
from prefect.schedules import Cron 
import threading


API_URL = "https://jsonplaceholder.typicode.com/posts" 

# TASK 1: INGEST DATA 
@task 
def ingest_data(): 
    response = requests.get(API_URL, timeout=30) 

    response.raise_for_status()  


    data = response.json()

    df = pd.DataFrame(data) 

    print("Records retrieved:", len(df)) 
    return df   

# TASK 2: VALIDATE DATA 
@task 
def validate_data(df):
        schema = pa.DataFrameSchema({ 
            "userId": pa.Column( 
                int, 
                checks=pa.Check.ge(1) 
            ), 
            "id": pa.Column( 
                int, 
                checks=pa.Check.ge(1), 
                unique=True 
            ), 
            "title": pa.Column( 
                str, 
                nullable=False, 
                checks=pa.Check.str_length(min_value=1) 
            ), 
            "body": pa.Column( 
                str, 
                nullable=False, 
                checks=pa.Check.str_length(min_value=1) 
            ) 
        })


        Validated_df = schema.validate(df) 
        print("Validation successful!") 
        return Validated_df

# TASK 3: TRANSFORM DATA 
@task 
def transform_data(df): 
        print("Transforming dataset...")

        df = df.copy() 
        df["ingestion_time"] = datetime.now(timezone.utc) 
    
        df = df[ 
            ["id", "userId", "title", "body", "ingestion_time"] 
        ] 
    
        return df   

# TASK 4: SAVE AS PARQUET 
@task 
def save_data(df): 
    os.makedirs("data", exist_ok=True) 
 
    file_path = "data/posts.parquet" 
 
    df.to_parquet( 
        file_path, 
        engine="pyarrow", 
        index=False 
    ) 
 
    print("Parquet file saved:", file_path) 
    return file_path

# MAIN PREFECT FLOW 
@flow(name="API Data Ingestion Pipeline") 
def data_pipeline(): 
    raw_data = ingest_data() 
    valid_data = validate_data(raw_data) 
    transformed_data = transform_data(valid_data) 
    save_data(transformed_data) 
    print("Pipeline completed successfully!")

# RUN THE FLOW 
if __name__ == "__main__": 
     
    daily_schedule = Cron( 
        "0 9 * * *", 
        timezone="Asia/Colombo" 
    )

    def start_prefect_server():
        current_dir = os.path.dirname(os.path.abspath(__file__))

        flow.from_source(
            source=current_dir,
            entrypoint="pipeline.py:data_pipeline"
        ).serve(
            name="API Data Ingestion Pipeline",
            schedules=[daily_schedule]
        )

    prefect_thread = threading.Thread(target=start_prefect_server,daemon=False)

    prefect_thread.start()

    print("Prefect scheduler started in a background thread.") 
    print("Scheduled execution: Every day at 09:00 AM.")  