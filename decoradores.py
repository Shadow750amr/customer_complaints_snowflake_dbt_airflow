import pandas as pd
import requests
import functools
import logging
import time

# Configuración de logging para ver los reintentos en consola
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

URL = 'https://repodatos.atdt.gob.mx/api_update/profeco/quejas_buro_comercial/buro_comercial_2019_2025.csv'
CSV_NAME = 'complaints_data.csv'

def retry_extraction(max_attempts=3, delay=2):
    """
    Decorador para manejar reintentos con espera exponencial.
    max_attempts: Número máximo de intentos.
    delay: Tiempo inicial de espera en segundos.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        logger.error(f"❌ Fallo tras {max_attempts} intentos: {e}")
                        raise # Lanza la excepción final para que Airflow/AWS lo detecten
                    
                    logger.warning(f"⚠️ Intento {attempts} fallido. Reintentando en {current_delay}s... Error: {e}")
                    time.sleep(current_delay)
                    current_delay *= 2  # Exponential Backoff
            return None
        return wrapper
    return decorator

class Extraction:
    def __init__(self, url: str, filename: str) -> None:
        self.url = url
        self.filename = filename
        logger.info("Clase Extraction instanciada correctamente.")

    @retry_extraction(max_attempts=3, delay=3)
    def connect_and_save(self) -> str:
        # 1. Timeout: Evita que el proceso se quede colgado en AWS (5s conexión, 60s lectura)
        with requests.get(self.url, stream=True, timeout=(5, 60)) as response:
            response.raise_for_status()
            
            # 2. Content-Length: Validación de contenido antes de procesar
            content_length = response.headers.get('Content-Length')
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                logger.info(f"Tamaño del archivo a descargar: {size_mb:.2f} MB")
            
            # 3. Descarga por chunks para proteger la RAM
            with open(self.filename, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
            
            logger.info(f"✅ Archivo {self.filename} generado con éxito.")
            return self.filename

if __name__ == "__main__":
    extractor = Extraction(URL, CSV_NAME)
    extractor.connect_and_save()