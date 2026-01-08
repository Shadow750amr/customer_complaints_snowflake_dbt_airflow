import snowflake.connector                                #  Standard import to handle snowflake connections
from snowflake.connector import SnowflakeConnection       # this was imported to define the connection return which is indeed a SnowflakeConnection
import os
import logging
from typing import Optional,Dict

logger = logging.getLogger(__name__) # Definiciòn del logger

class SnowflakeConnector:

    ''' This class helps define a connection to snowflake using the following methods:
        _get_connection: lazy initialization to define a connection to snowflake
        upload_to_stage: this uploads the csv to the stage
        ingest_from_stage: this get the uploaded csv and then transport it to the defined table (raw table in bronze layer)
        create_table: this creates the bronze raw table based on the table schema from the stage
        close: this closes the connection to snowflake making sure there is no active connection (really important to switch off the wh)  '''

    def __init__(self, user: str, account: str, warehouse: str, 
                 database: str, schema: str, password: Optional[str] = None) -> None:
        
        self.conn_params = {
            "user": user,
            "password": password or os.getenv('SNOWFLAKE_PASSWORD'),
            "account": account,
            "warehouse": warehouse,
            "database": database,
            "schema": schema
        }
        self.conn = None

    def _get_connection(self) -> SnowflakeConnection:          #Lazy initialization
        if self.conn is None:
            self.conn = snowflake.connector.connect(**self.conn_params)         # ** to return the list of arguments listed above with no mistakes
        return self.conn

    def upload_to_stage(self, local_path: str, stage_name: str) -> str:
        cursor = self._get_connection().cursor()
        try:
            logger.info(f"Subiendo {local_path} al stage {stage_name}...")
            put_query = f"PUT file://{local_path} @{stage_name} OVERWRITE = TRUE"
            cursor.execute(put_query)
        finally:
            cursor.close()
    
    def create_table(self,table_name: str, stage_name: str) -> str:
        cursor = self._get_connection().cursor()
        try:
            logger.info("Creando tabla base")
            create_query = f"""
            CREATE OR REPLACE TABLE {table_name}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '@{stage_name}',
                        FILE_FORMAT => 'CSV'
                    )
                )
            )
            """
            cursor.execute(create_query)
        except Exception as e:
            logger.error(f"No hay tabla {e}")
                    
        finally:
            cursor.close()

    def ingest_from_stage(self, table_name: str, stage_name: str) -> str:

        cursor = self._get_connection().cursor()
        try:
            logger.info(f"Cargando datos a la tabla {table_name}...")
            copy_query =f"""
            COPY INTO {table_name}
            FROM @{stage_name}
            FILE_FORMAT = (
            FORMAT_NAME = 'CSV')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
            """ 
            cursor.execute(copy_query)
        finally:
            cursor.close()

    def close(self):
        if self.conn:
            self.conn.close()




if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO)
    from dotenv import load_dotenv
    load_dotenv()


    sf = SnowflakeConnector(
        user=os.getenv('SNOWFLAKE_USER'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    try:
     
        path_archivo = os.getenv('LOCAL_FILE_PATH')
        nombre_stage = os.getenv('SNOWFLAKE_STAGE_NAME')
        tabla_destino = "COMPLAINTS_DB.COMPLAINTS_RAW.BRONZE_COMPLAINTS"

        sf.upload_to_stage(path_archivo, nombre_stage)

        sf.create_table(tabla_destino, nombre_stage)
        
        sf.ingest_from_stage(tabla_destino, nombre_stage)
        
        print("--- PRUEBA FINALIZADA CON ÉXITO ---")

    except Exception as e:
        print(f"--- LA PRUEBA FALLÓ: {e} ---")
    
    finally:
        
        sf.close()



