import snowflake.connector                                #  Standard import to handle snowflake connections
#from snowflake.connector import SnowflakeConnection          # this was imported to define the connection return which is indeed a SnowflakeConnection
import os
import logging
from typing import Optional,Dict

class SnowflakeConnector:
    logger = logging.getLogger(__name__) # Definiciòn del logger

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

    #def _get_connection(self) -> SnowflakeConnection:          #Lazy initialization
    #   if self.conn is None:
    #      self.conn = snowflake.connector.connect(**self.conn_params)         # ** to return the list of arguments listed above with no mistakes
    #  return self.conn

    def upload_to_stage(self, local_path: str, stage_name: str):
        conn = self.conn.snowflake.connector.connect(**self.conn_params)
        cursor = conn.cursor()
        try:
            self.logger.info(f"Subiendo {local_path} al stage {stage_name}...")
            put_query = f"PUT file://{local_path} @{stage_name} OVERWRITE = TRUE"
            cursor.execute(put_query)
        finally:
            cursor.close()

    def ingest_from_stage(self, table_name: str, stage_name: str):
        """Paso 2: COPY INTO - Mueve los datos del stage a la tabla"""
        conn = self.conn.snowflake.connector.connect(**self.conn_params)
        cursor = conn.cursor()
        try:
            self.logger.info(f"Cargando datos en la tabla {table_name}...")
            copy_query = f"""
            COPY INTO {table_name}
            FROM @{stage_name}
            FILE_FORMAT = 'CSV'
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
            """ 
            cursor.execute(copy_query)
        finally:
            cursor.close()

    def close(self):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    # 1. Configurar el logging para ver qué pasa en la consola
    logging.basicConfig(level=logging.INFO)
    from dotenv import load_dotenv
    load_dotenv()

    # 2. Instanciar la clase con las variables de entorno
    # Esto simula lo que haría un Operador de Airflow
    sf = SnowflakeConnector(
        user=os.getenv('SNOWFLAKE_USER'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    try:
        # 3. Ejecutar el flujo paso a paso
        path_archivo = os.getenv('LOCAL_FILE_PATH')
        nombre_stage = os.getenv('SNOWFLAKE_STAGE_NAME')
        tabla_destino = "bronze_complaints"

        # Paso A: Subir al Stage
        sf.upload_to_stage(path_archivo, nombre_stage)

        # Paso B: Cargar a la tabla
        sf.ingest_from_stage(tabla_destino, nombre_stage)
        
        print("--- PRUEBA FINALIZADA CON ÉXITO ---")

    except Exception as e:
        print(f"--- LA PRUEBA FALLÓ: {e} ---")
    
    finally:
        # 4. Limpiar la conexión (muy importante en Snowflake para no gastar créditos)
        sf.close()