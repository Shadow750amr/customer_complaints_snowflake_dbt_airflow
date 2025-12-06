import pandas as pd
import requests


# Deprecated urls belong to policy changes in the data consumption defined by Mexican Government.
# Anyway we include this dataset as is.

# Defined url
url = 'https://www.datos.gob.mx/dataset/bc7415fe-1a90-4d3a-a28b-d6fe4da652ab/resource/9cb010b2-ddf3-4dfc-8e5e-286fcaec1a0a/download/movimiento_operacional_pasajeros_comerciales_aicm_ok.csv'


# Calling the url and chunksizing for efficiency
with open(r'mov_operacional.csv','wb') as csv_file:
    response = requests.get(url,stream=True)
    response.raise_for_status()
    for chunk in response.iter_content(chunk_size=8192):
        csv_file.write(chunk)

# Validating data was succesfully exported to csv
df = pd.read_csv(r'mov_operacional.csv').head(5)
print(df)

# Also validating datatypes
print(df.info())











