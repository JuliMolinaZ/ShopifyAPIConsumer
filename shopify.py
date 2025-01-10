import logging
import random
import time
# Función para obtener el nombre de la tienda (puede ajustarse según necesidad)
def get_store_name(store_url):
    # Asumimos que store_url ya es el nombre de la tienda sin 'https://'
    return store_url

# Función para obtener encabezados de autenticación
def get_auth_headers(api_key, password):
    """
    Genera los encabezados de autenticación para la API de Shopify.

    Args:
        api_key (str): Clave de API de Shopify.
        password (str): Contraseña o token de acceso de la API.

    Returns:
        dict: Encabezados para solicitudes HTTP.
    """
    return {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': password
    }


# shopify_utils.py
def calculate_wait_time(attempt, backoff_factor, retry_after, max_wait_time):
    """
    Calcula el tiempo de espera con un factor de retroceso exponencial y jitter.

    Args:
        attempt (int): Número de intento actual.
        backoff_factor (float): Factor de retroceso.
        retry_after (int): Tiempo de espera sugerido por el servidor (en segundos).
        max_wait_time (int): Tiempo máximo permitido para esperar.

    Returns:
        float: Tiempo de espera en segundos.
    """
    base_wait = min(backoff_factor ** attempt * retry_after, max_wait_time)
    return base_wait / 2 + random.uniform(0, base_wait / 2)

# Función para manejar rate limiting
def handle_rate_limiting(response, attempt, backoff_factor=2, max_wait_time=60):
    """
    Maneja respuestas de límite de tasa (HTTP 429) con tiempo de espera y reintentos.

    Args:
        response (Response): Respuesta HTTP de la API.
        attempt (int): Número de intento actual.
        backoff_factor (float): Factor de retroceso (predeterminado: 2).
        max_wait_time (int): Tiempo máximo permitido para esperar (predeterminado: 60).

    Returns:
        bool: Verdadero si se debe reintentar, falso en caso contrario.
    """
    if response.status_code == 429:
        retry_after = int(float(response.headers.get('Retry-After', 4)))
        wait_time = calculate_wait_time(attempt, backoff_factor, retry_after, max_wait_time)
        logging.warning(f"Límite de tasa excedido. Reintentando después de {wait_time:.2f} segundos.")
        time.sleep(wait_time)
        return True
    return False


# Función para registrar el límite de llamadas a la API
def log_api_call(response):
    """
    Registra el uso del límite de llamadas de la API.

    Args:
        response (Response): Respuesta HTTP de la API.
    """
    call_limit = response.headers.get('X-Shopify-Shop-Api-Call-Limit')
    if call_limit:
        try:
            used, total = map(int, call_limit.split('/'))
            logging.debug(f"API Call Limit: {used}/{total}")
        except ValueError:
            logging.warning(f"Formato inesperado en 'X-Shopify-Shop-Api-Call-Limit': {call_limit}")
    else:
        logging.info("El encabezado 'X-Shopify-Shop-Api-Call-Limit' no está presente.")

