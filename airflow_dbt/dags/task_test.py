from airflow.decorators import dag,task,task_group
from src.data_extraction import Extraction
from src.upload_to_s3 import upload_file
import datetime
import os


URL = 'https://repodatos.atdt.gob.mx/api_update/profeco/quejas_buro_comercial/buro_comercial_2019_2025.csv'
CSV_NAME = r'/usr/local/airflow/complaints_data.csv'


@dag(start_date=datetime.datetime(2026, 5, 5),schedule='@daily',catchup=False)
def complaints_pipeline():
    @task(task_id="ejecutar_descarga")
    def descarga_archivo():
        descargar_archivo = Extraction(URL,CSV_NAME)
        path_archivo = descargar_archivo.connect_and_save()
        return path_archivo

    @task(task_id="cargar_s3")
    def mandar_archivo_s3(path_archivo):
        return upload_file(
            file_name=path_archivo,
            bucket = os.getenv('BUCKET_NAME'),
            aws_conn_id='aws_default'
        )
    file_path_output = descarga_archivo()
    mandar_archivo_s3(file_path_output)

complaints_pipeline()

        