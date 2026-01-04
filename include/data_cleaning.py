import pandas as pd
import logging


# Defining the path where the csv is located at
PATH = '/Users/shadow750/Documents/datawh_certification/complaints_data.csv'

# Defining the columns for further transformations
TEXT_COLUMNS = [
    "expediente", "estado_procesal", "razon_social", 
    "nombre_comercial", "giro", "sector", 
    "area_responsable", "estado", "motivo_reclamacion"
]

DATE_COLUMNS = ["fecha_ingreso"]

class DataCleaning:

    def __init__(self,path:str,text_cols:list,date_cols:list) -> None:
        self.path = path
        self.text_cols = text_cols
        self.date_cols = date_cols
        self.df = None

    def clean_data(self) -> pd.DataFrame:

        # Read the csv
        self.df = pd.read_csv(self.path)
        # string columns
        for col in self.text_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str).str.strip()
        #  date columns
        for col in self.date_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col],errors='coerce')
        return self.df

if __name__ == "__main__":
    data = DataCleaning(PATH,TEXT_COLUMNS,DATE_COLUMNS)
    final_df = data.clean_data()
    print(final_df.head(5))

