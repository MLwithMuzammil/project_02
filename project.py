from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
import json
import csv
from io import StringIO
from datetime import datetime, timedelta

# Function to transform the Spotify data
def transform_spotify_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='get_raw_data')
    tracks = data.get('tracks', [])
    transformed_data = []
    
    for track in tracks:
        track_info = {
            "Track Name": track.get("name", ""),
            "Album Name": track.get("album", {}).get("name", ""),
            "Artist Name": ", ".join([artist.get("name", "") for artist in track.get("artists", [])]),
            "Duration (ms)": track.get("duration_ms", 0),
            "Popularity": track.get("popularity", 0),
            "Spotify URL": track.get("external_urls", {}).get("spotify", "")
        }
        transformed_data.append(track_info)
    
    return transformed_data

# Function to write transformed data to CSV
def write_to_csv(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_spotify_data')
    csv_buffer = StringIO()
    fieldnames = ["Track Name", "Album Name", "Artist Name", "Duration (ms)", "Popularity", "Spotify URL"]
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    
    writer.writeheader()
    writer.writerows(transformed_data)
    csv_content = csv_buffer.getvalue()
    
    return csv_content

# Function to fetch raw data from S3
def get_raw_data(**kwargs):
    s3_hook = S3Hook(
        aws_access_key_id='',
        aws_secret_access_key='',
        region_name=''
    )
    
    source_bucket = "spotify-raw-01"
    source_key = kwargs['source_key']
    
    # Fetch the file from S3
    response = s3_hook.get_key(source_key, source_bucket)
    if response is None:
        print(f"Failed to fetch file: {source_key} from {source_bucket}")
        return "Error: File not found."
    
    # Read the raw data from the file
    raw_data = json.loads(response.get()['Body'].read())
    
    return raw_data

# Function to upload transformed data to S3
def upload_transformed_data_to_s3(**kwargs):
    s3_hook = S3Hook(
        aws_access_key_id='your-access-key',
        aws_secret_access_key='your-secret-key',
        region_name='your-region'
    )
    
    transformed_data = kwargs['ti'].xcom_pull(task_ids='write_to_csv')
    target_bucket = "spotify-transform-02"
    target_key = kwargs['target_key']
    
    # Upload the CSV file to the target S3 bucket
    s3_hook.load_string(transformed_data, key=target_key, bucket_name=target_bucket, replace=True)
    
    return f"File transformed and saved to {target_bucket}/{target_key}"

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spotify_data_transform',
    default_args=default_args,
    description='A simple DAG to transform Spotify data from JSON to CSV',
    schedule_interval=None,  # Trigger manually or set a schedule
    start_date=datetime(2025, 1, 6),
    catchup=False,  # Prevent backfilling
)

# Define the PythonOperator for the task to get raw data from S3
get_raw_data_task = PythonOperator(
    task_id='get_raw_data',
    python_callable=get_raw_data,
    op_kwargs={'source_key': 'path/to/your/file.json'},  # Example source key, update as needed
    dag=dag,
)

# Define the PythonOperator for the task to transform the data
transform_spotify_data_task = PythonOperator(
    task_id='transform_spotify_data',
    python_callable=transform_spotify_data,
    provide_context=True,  # Pass the context to the function
    dag=dag,
)

# Define the PythonOperator for the task to write transformed data to CSV
write_to_csv_task = PythonOperator(
    task_id='write_to_csv',
    python_callable=write_to_csv,
    provide_context=True,  # Pass the context to the function
    dag=dag,
)

# Define the PythonOperator for the task to upload transformed data to S3
upload_transformed_data_to_s3_task = PythonOperator(
    task_id='upload_transformed_data_to_s3',
    python_callable=upload_transformed_data_to_s3,
    op_kwargs={'target_key': 'path/to/your/target/file.csv'},  # Example target key, update as needed
    provide_context=True,  # Pass the context to the function
    dag=dag,
)

# Set task dependencies to ensure proper execution order
get_raw_data_task >> transform_spotify_data_task >> write_to_csv_task >> upload_transformed_data_to_s3_task
