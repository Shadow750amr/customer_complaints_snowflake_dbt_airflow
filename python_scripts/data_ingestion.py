import snowflake.connector
import os
from dotenv import load_dotenv


# we use the load_dotenv to read from the .env file
load_dotenv()

# and define the function upload_only that get the variables from the .env file and use these credential to connect to snowflake.
# also, we run the PUT statement that picks the csv and ingest it into snowflake (internal stage of course)

def upload_only():
    
    # env variables
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    local_path = os.getenv('LOCAL_FILE_PATH')
    stage_name = os.getenv('SNOWFLAKE_STAGE_NAME')
    
        # making the connection using the variables
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        # crating the cursor to execute statements
        cursor = conn.cursor()
        
        # Using PUT with f strings 
        # Since it is an schema stage, we use @DB.SCHEMA.STAGE 
        # If it was a table stage then we may use @%TABLE_STAGE but is not the case

        print(f"uploading {local_path} a {stage_name}...")
        
        put_query = f"PUT file://{local_path} @{database}.{schema}.{stage_name} OVERWRITE = TRUE"
        
        cursor.execute(put_query)
        
        copy_query = '''CREATE OR REPLACE TABLE COMPLAINTS_RAW.bronze_complaints 
                            USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) 
                                                FROM TABLE(INFER_SCHEMA(LOCATION=>'@COMPLAINTS_DB.COMPLAINTS_RAW.DBT_STAGE', FILE_FORMAT=>'CSV'))); '''
        cursor.execute(copy_query)

        load_data_query = f"""
        COPY INTO {database}.{schema}.bronze_complaints
        FROM @{database}.{schema}.{stage_name}
        FILE_FORMAT = 'CSV'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
        cursor.execute(load_data_query)
        
        print("Archivo cargado con éxito.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    upload_only()


