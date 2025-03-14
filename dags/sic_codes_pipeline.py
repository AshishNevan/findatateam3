from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/dags/scripts/')

# Import the scraping function
from scripts.scrape_sic_codes import scrape_sic_codes

SNOWFLAKE_CONN_ID = 'snowflake_default'
AWS_CONN_ID = 'aws_default'
BUCKET_NAME = 'findata-test'
S3_KEY = 'sic_codes/sic_codes.csv'

default_args = {
    'owner': 'findata_team',
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

CREATE_SIC_TABLE = """
CREATE TABLE IF NOT EXISTS FINDATA_RAW.REFERENCE.SIC_CODES (
    SIC_CODE STRING,
    INDUSTRY_NAME STRING
);
"""

COPY_INTO_SIC_TABLE = """
COPY INTO FINDATA_RAW.REFERENCE.SIC_CODES
FROM @FINDATA_RAW.REFERENCE.SIC_STAGE/sic_codes.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
"""

def upload_to_s3(**kwargs):
    """Upload SIC codes CSV to S3."""
    try:
        # Run the scraping function
        csv_path = scrape_sic_codes()
        
        # Upload to S3
        s3_hook = S3Hook(AWS_CONN_ID)
        s3_hook.load_file(csv_path, S3_KEY, BUCKET_NAME, replace=True)
        print(f"Uploaded SIC codes to s3://{BUCKET_NAME}/{S3_KEY}")
        
        return True
    except Exception as e:
        print(f"Error uploading SIC codes to S3: {str(e)}")
        raise

def create_snowflake_objects(**kwargs):
    """Create Snowflake schema, stage and table."""
    try:
        # Get S3 Hook
        s3_hook = S3Hook(AWS_CONN_ID)
        aws_credentials = s3_hook.get_credentials()
        aws_key_id = aws_credentials.access_key
        aws_secret_key = aws_credentials.secret_key
        
        # Create schema and stage
        create_schema_sql = """
        CREATE SCHEMA IF NOT EXISTS FINDATA_RAW.REFERENCE;
        
        CREATE STAGE IF NOT EXISTS FINDATA_RAW.REFERENCE.SIC_STAGE
        URL = 's3://findata-test/sic_codes/'
        CREDENTIALS = (AWS_KEY_ID = '{aws_key_id}' AWS_SECRET_KEY = '{aws_secret_key}')
        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
        """.format(aws_key_id=aws_key_id, aws_secret_key=aws_secret_key)
        
        snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
        snowflake_hook.run(create_schema_sql)
        
        # Create table
        snowflake_hook.run(CREATE_SIC_TABLE)
        
        return True
    except Exception as e:
        print(f"Error creating Snowflake objects: {str(e)}")
        raise

def load_data_to_snowflake(**kwargs):
    """Load SIC codes data into Snowflake."""
    try:
        snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
        
        # First truncate the table to ensure fresh data
        truncate_sql = "TRUNCATE TABLE FINDATA_RAW.REFERENCE.SIC_CODES;"
        snowflake_hook.run(truncate_sql)
        
        # Load data from S3 stage
        snowflake_hook.run(COPY_INTO_SIC_TABLE)
        
        # Verify data was loaded
        result = snowflake_hook.get_first("SELECT COUNT(*) FROM FINDATA_RAW.REFERENCE.SIC_CODES;")
        print(f"Loaded {result[0]} SIC codes into Snowflake")
        
        return True
    except Exception as e:
        print(f"Error loading data to Snowflake: {str(e)}")
        raise

with DAG(
    'sic_codes_pipeline',
    default_args=default_args,
    description='Pipeline to scrape SIC codes and load them into Snowflake',
    schedule_interval='@monthly',  # Run monthly to keep data fresh
    catchup=False
) as dag:
    
    scrape_and_upload_task = PythonOperator(
        task_id='scrape_and_upload_sic_codes',
        python_callable=upload_to_s3
    )
    
    create_snowflake_objects_task = PythonOperator(
        task_id='create_snowflake_objects',
        python_callable=create_snowflake_objects
    )
    
    load_data_task = PythonOperator(
        task_id='load_sic_data_to_snowflake',
        python_callable=load_data_to_snowflake
    )
    
    # Define task dependencies
    scrape_and_upload_task >> create_snowflake_objects_task >> load_data_task