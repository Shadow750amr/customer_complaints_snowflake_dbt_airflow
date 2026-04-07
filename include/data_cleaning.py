import pandas as pd
import logging
import os
from dotenv import load_dotenv
load_dotenv()
from utils.formats import TEXT_COLUMNS,DATE_COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log", mode='w'),
        logging.StreamHandler() # Para ver logs en consola simultáneamente
    ]
)
logger = logging.getLogger(__name__)


# Defining the path where the csv is located at
PROCESSED_CSV = os.getenv('PROCESSED_CSV')
TEXT_COLUMNS = TEXT_COLUMNS
DATE_COLUMNS = DATE_COLUMNS

class DataCleaning:

    def __init__(self,path:str,text_cols:list,date_cols:list) -> None:
        self.path = path
        self.text_cols = text_cols
        self.date_cols = date_cols
        self.df = None
    
    def clean_data(self) -> pd.DataFrame:

        logger.info(f"Initializing cleaning for file {self.path}")

    # Read the csv
        self.df = pd.read_csv(self.path)
        try:
    # string columns
            for col in self.text_cols:
                if col in self.df.columns:
                    self.df[col] = self.df[col].astype(str).str.strip()
    #  date columns
            for col in self.date_cols:
                if col in self.df.columns:
                    self.df[col] = pd.to_datetime(self.df[col],errors='coerce')
                    return self.df
        except Exception as e:
            logger.error(f"Could not clean data {e}")
    


if __name__ == "__main__":
    data = DataCleaning(PROCESSED_CSV,TEXT_COLUMNS,DATE_COLUMNS)
    final_df = data.clean_data()
    print(final_df.head(5))

