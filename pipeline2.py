import requests 
import pandas as pd
import pandera.pandas as pa
from datetime import datetime, timezone 
import os  
from prefect import flow, task    
from prefect.schedules import Cron 
import threading


API_URL = "https://jsonplaceholder.typicode.com/users" 

# TASK 1: INGEST DATA 
@task 
def ingest_data(): 
    response = requests.get(API_URL, timeout=30) 

    response.raise_for_status()  


    data = response.json()

    # Remove df = pd.DataFrame(data) and add this line to flatten the nested JSON structure
    df = pd.json_normalize(data) 

    print("Records retrieved:", len(df)) 
    return df   

# TASK 2: VALIDATE DATA 
@task 
def validate_data(df):
    schema = pa.DataFrameSchema({
        "id": pa.Column(int, checks=pa.Check.ge(1), unique=True),
        "name": pa.Column(str, nullable=False, checks=pa.Check.str_length(min_value=1)),
        "username": pa.Column(str, nullable=False, unique=True, checks=pa.Check.str_length(min_value=1)),
        "email": pa.Column(
            str,
            nullable=False,
            checks=pa.Check.str_matches(r"^[\w\.-]+@[\w\.-]+\.\w+$")
        ),
        "phone": pa.Column(str, nullable=False, checks=pa.Check.str_length(min_value=1)),
        "website": pa.Column(str, nullable=False, checks=pa.Check.str_length(min_value=1)),

        # Flattened address fields
        "address.street": pa.Column(str, nullable=False),
        "address.suite": pa.Column(str, nullable=False),
        "address.city": pa.Column(str, nullable=False),
        "address.zipcode": pa.Column(str, nullable=False),

        # Flattened geo fields (coerced from string to float for range checks)
        "address.geo.lat": pa.Column(
            float,
            coerce=True,
            checks=pa.Check.in_range(-90.0, 90.0)
        ),
        "address.geo.lng": pa.Column(
            float,
            coerce=True,
            checks=pa.Check.in_range(-180.0, 180.0)
        ),

        # Flattened company fields
        "company.name": pa.Column(str, nullable=False),
        "company.catchPhrase": pa.Column(str, nullable=False),
        "company.bs": pa.Column(str, nullable=False),
    })


    Validated_df = schema.validate(df) 
    print("Validation successful!") 
    return Validated_df 



# TASK 3: TRANSFORM DATA 
@task
def transform_data(df):
    print("Transforming dataset...")

    df = df.copy()
    
    # Replace dots in column names with underscores for easier access
    df.columns = df.columns.str.replace(".", "_", regex=False)
    
    
    df["ingestion_time"] = datetime.now(timezone.utc)

    # Reorder and select the desired columns for the final DataFrame
    df = df[
        [
            "id",
            "name",
            "username",
            "email",
            "phone",
            "website",
            "address_city",
            "address_zipcode",
            "address_geo_lat",
            "address_geo_lng",
            "company_name",
            "company_catchPhrase",
            "company_bs",
            "ingestion_time",
        ]
    ]

    return df

# TASK 4: SAVE AS PARQUET 
@task 
def save_data(df): 
    os.makedirs("data", exist_ok=True) 
 
    file_path = "data/users.parquet" 
 
    df.to_parquet( 
        file_path, 
        engine="pyarrow", 
        index=False 
    ) 
 
    print("Parquet file saved:", file_path) 
    return file_path


# MAIN PREFECT FLOW 
@flow(name="user-data-pipeline")
def data_pipeline():
    raw_df = ingest_data()
    validated_df = validate_data(raw_df)
    transformed_df = transform_data(validated_df)
    file_path = save_data(transformed_df)
    print("Pipeline completed successfully!")
    return file_path

if __name__ == "__main__":
    file_path = data_pipeline()

    # Read the Parquet file back using Pandas
    df_read = pd.read_parquet(file_path, engine="pyarrow")

    # Display the first five records
    print("\nFirst 5 records:")
    print(df_read.head(5))

    # Confirm the record count
    print("\nTotal record count:", len(df_read))

    daily_schedule = Cron( 
            "0 * * * *", 
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
    print("Scheduled execution: Every hour on the hour (minute 00).")

