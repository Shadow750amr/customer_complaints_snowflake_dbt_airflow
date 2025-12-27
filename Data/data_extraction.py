import pandas as pd
import requests


# Deprecated urls belong to policy changes in the data consumption defined by Mexican Government.
# Anyway we include this dataset as is.

# Defined url
url = 'https://repodatos.atdt.gob.mx/api_update/profeco/quejas_buro_comercial/buro_comercial_2019_2025.csv'


# Calling the url and chunksizing for efficiency
with open(r'complaints_data.csv','wb') as csv_file:
    response = requests.get(url,stream=True)
    response.raise_for_status()
    for chunk in response.iter_content(chunk_size=8192):
        csv_file.write(chunk)

# Validating data was succesfully exported to csv
df = pd.read_csv(r'complaints_data.csv').head(5)
print(df)

# Also validating datatypes
print(df.info())











