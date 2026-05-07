import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
import os


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log", mode='w'),
        logging.StreamHandler() # Para ver logs en consola simultáneamente
    ]
)
logger = logging.getLogger(__name__)



def upload_file(file_name:str, bucket:str, aws_conn_id='aws_default'):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    logger.info("Initializing upload stage to S3.")
    # If S3 object_name was not specified, use file_name
    # Upload the file
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    try:
        response = s3_hook.load_file(filename=file_name,key='aws_file.csv', bucket_name=bucket,replace=True)
    except ClientError as e:
        logging.error(e)
        return False
    logger.info("Process done.")
    return 
    
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    CSV_NAME = '/Users/shadow750/Documents/datawh_certification/include/complaints_data.csv'
    upload_file(file_name=CSV_NAME,bucket=os.getenv('BUCKET_NAME'),aws_conn_id='aws_default')