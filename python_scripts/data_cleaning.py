import pandas as pd

# Defining the columns for further transformations
text_columns = [
    "expediente", "estado_procesal", "razon_social", 
    "nombre_comercial", "giro", "sector", 
    "area_responsable", "estado", "motivo_reclamacion"
]
# Defining the path where the csv is located at
path = '/Users/shadow750/Documents/datawh_certification/complaints_data.csv'

# Read the csv
df = pd.read_csv(path)

# First, transforming the column 'fecha_ingreso' (date column reference) to datetime
df["fecha_ingreso"] = pd.to_datetime(df["fecha_ingreso"], errors='coerce')

# now the rest of the columns to str for better performance

df[text_columns] = df[text_columns].astype("string")

# also, delete the white spaces

for col in text_columns:
    df[col] = df[col].str.strip()


print(df.info())
print(len(df))
