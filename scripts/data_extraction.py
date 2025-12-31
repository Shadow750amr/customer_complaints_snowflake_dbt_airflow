import requests
import logging

# Configuración de logging para ver qué pasa
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = 'https://repodatos.atdt.gob.mx/api_update/profeco/quejas_buro_comercial/buro_comercial_2019_2025.csv'
CSV_NAME = 'complaints_data.csv'

class Extraction:
    def __init__(self, url: str, filename: str) -> None:
        self.url = url
        self.filename = filename
        logger.info("Clase Extraction instanciada correctamente.")
        return None

    def connect_and_save(self) ->str:
        try:
            with requests.get(self.url, stream=True) as response:
                response.raise_for_status()
                with open(self.filename, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
            logger.info(f"Archivo {self.filename} generado con éxito.")
            return self.filename
        except Exception as e:
            logger.error(f"Error en la descarga: {e}")

if __name__ == "__main__":
    # Aquí es donde se pasaban los argumentos que daban error
    extractor = Extraction(URL, CSV_NAME)
    extractor.connect_and_save()