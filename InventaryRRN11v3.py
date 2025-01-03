import requests
import mysql.connector
import time
import logging
import json
import random
import os
from dotenv import load_dotenv
from collections import deque
from math import ceil
import unicodedata

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de Logging
logging.basicConfig(
    filename='inventario_sync.log',
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Configuración de Shopify API desde .env
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_PASSWORD = os.getenv('SHOPIFY_API_PASSWORD')
SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')

# Configuración de MySQL desde .env
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', 3306)
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Ubicaciones Deseadas (nombres tal como aparecen en Shopify)
DESIRED_LOCATIONS = [
    'CANCÚN',
    'CEDIS',
    'EXPERIENCIA',
    'GUADALAJARA',
    'LIVERPOOL',
    'ONLINE',
    'RAYONETA 1.0',
    'RAYONETA 2.0'
]

# Clase para manejar el Rate Limiting de Shopify
class RateLimiter:
    def __init__(self, max_calls, period):
        self.calls = deque()
        self.max_calls = max_calls
        self.period = period  # en segundos

    def wait(self):
        current = time.time()
        while self.calls and self.calls[0] <= current - self.period:
            self.calls.popleft()
        if len(self.calls) >= self.max_calls:
            wait_time = self.period - (current - self.calls[0])
            # Añadir jitter
            wait_time = wait_time / 2 + random.uniform(0, wait_time / 2)
            logging.info(f"Rate limit alcanzado. Esperando {wait_time:.2f} segundos.")
            time.sleep(wait_time)
        self.calls.append(time.time())

# Función para limpiar strings
def clean_string(s):
    if s:
        return s.strip().replace("'", "''")
    return 'Unknown'

# Función para validar barcode (puedes ajustar según tus reglas)
def validate_barcode(barcode):
    if barcode and barcode != '':
        return True
    return False

# Función para crear conexión a MySQL
def create_db_connection():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        logging.info("Conexión a la base de datos establecida.")
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Error al conectar a la base de datos: {err}")
        raise

# Función para obtener el nombre de la tienda (puede ajustarse según necesidad)
def get_store_name(store_url):
    # Asumimos que store_url ya es el nombre de la tienda sin 'https://'
    return store_url

# Función para obtener encabezados de autenticación
def get_auth_headers(api_key, password):
    return {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': password
    }

# Función para manejar rate limiting
def handle_rate_limiting(response, attempt, backoff_factor, max_wait_time):
    if response.status_code == 429:
        retry_after = int(float(response.headers.get('Retry-After', 4)))
        wait_time = min(backoff_factor ** attempt * retry_after, max_wait_time)
        # Añadir jitter
        wait_time = wait_time / 2 + random.uniform(0, wait_time / 2)
        logging.warning(f"Límite de tasa excedido. Reintentando después de {wait_time:.2f} segundos.")
        time.sleep(wait_time)
        return True
    return False

# Función para registrar el límite de llamadas a la API
def log_api_call(response):
    call_limit = response.headers.get('X-Shopify-Shop-Api-Call-Limit')
    if call_limit:
        used, total = map(int, call_limit.split('/'))
        logging.debug(f"API Call Limit: {used}/{total}")

# Función para normalizar strings (eliminar acentos y convertir a mayúsculas)
def normalize_string(s):
    if not s:
        return ''
    nfkd_form = unicodedata.normalize('NFKD', s)
    only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('utf-8')
    return only_ascii.upper()

# Función para obtener ubicaciones desde Shopify
def fetch_locations(api_key, password, store_name, api_version='2023-07'):
    base_url = f"https://{store_name}/admin/api/{api_version}/locations.json"
    headers = get_auth_headers(api_key, password)
    session = requests.Session()
    locations = {}
    rate_limiter = RateLimiter(max_calls=4, period=1)  # Ajustar para 4 llamadas por segundo

    max_retries = 5
    backoff_factor = 2
    max_wait_time = 60

    for attempt in range(max_retries):
        try:
            rate_limiter.wait()
            response = session.get(base_url, headers=headers)
            log_api_call(response)
            if response.status_code == 200:
                data = response.json()
                for loc in data.get('locations', []):
                    loc_id = loc.get('id')
                    loc_name = loc.get('name')
                    locations[loc_id] = loc_name
                    logging.info(f"Location ID: {loc_id}, Nombre original: {loc_name}")
                logging.info(f"Obtenidas {len(locations)} ubicaciones.")
                return locations
            elif response.status_code == 429:
                logging.error(f"Error al obtener ubicaciones: {response.status_code} {response.text}")
                if handle_rate_limiting(response, attempt, backoff_factor, max_wait_time):
                    continue
            else:
                logging.error(f"Error al obtener ubicaciones: {response.status_code} {response.text}")
                wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                logging.info(f"Esperando {wait_time} segundos antes de reintentar.")
                time.sleep(wait_time)
        except requests.RequestException as e:
            wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
            logging.warning(f"Error en la solicitud HTTP para ubicaciones (intento {attempt+1}): {e}. Reintentando después de {wait_time} segundos.")
            time.sleep(wait_time)

    logging.error("Máximo número de intentos alcanzado para obtener ubicaciones. No se obtuvieron ubicaciones.")
    return locations  # Devolverá vacío

# Función para filtrar las ubicaciones deseadas
def get_desired_location_ids(locations, desired_names):
    desired_ids = []
    normalized_desired_names = [normalize_string(name) for name in desired_names]
    for loc_id, loc_name in locations.items():
        normalized_loc_name = normalize_string(loc_name)
        if normalized_loc_name in normalized_desired_names:
            desired_ids.append(loc_id)
    return desired_ids

# Función para obtener productos activos desde Shopify
def fetch_shopify_products(api_key, password, store_name, api_version='2023-07'):
    base_url = f"https://{store_name}/admin/api/{api_version}/products.json"
    headers = get_auth_headers(api_key, password)
    session = requests.Session()
    products = []
    limit = 250  # Máximo permitido por Shopify
    url = f"{base_url}?limit={limit}&status=active"
    rate_limiter = RateLimiter(max_calls=4, period=1)  # Ajustar para 4 llamadas por segundo
    max_retries = 5
    backoff_factor = 2
    max_wait_time = 60

    while url:
        for attempt in range(max_retries):
            try:
                rate_limiter.wait()
                response = session.get(url, headers=headers)
                log_api_call(response)
                if response.status_code == 200:
                    data = response.json()
                    fetched_products = data.get('products', [])
                    products.extend(fetched_products)
                    logging.info(f"Se han obtenido {len(fetched_products)} productos.")
                    # Manejar paginación
                    link_header = response.headers.get('Link')
                    if link_header:
                        links = link_header.split(',')
                        url = None
                        for link in links:
                            if 'rel="next"' in link:
                                start = link.find('<') + 1
                                end = link.find('>')
                                if start > 0 and end > start:
                                    next_url = link[start:end]
                                    url = next_url
                                break
                    else:
                        url = None
                    break
                elif response.status_code == 429:
                    logging.error(f"Error inesperado al obtener productos: {response.status_code} {response.text}")
                    if handle_rate_limiting(response, attempt, backoff_factor, max_wait_time):
                        continue
                else:
                    logging.warning(f"Error inesperado al obtener productos (intento {attempt+1}): {response.status_code} {response.text}. Reintentando.")
                    wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                    logging.info(f"Esperando {wait_time} segundos antes de reintentar.")
                    time.sleep(wait_time)
            except requests.RequestException as e:
                wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                logging.warning(f"Error en la solicitud HTTP para productos (intento {attempt+1}): {e}. Reintentando después de {wait_time} segundos.")
                time.sleep(wait_time)
        else:
            logging.error("Máximo número de intentos alcanzado. Saliendo del fetch de productos.")
            break

        logging.info(f"Procesado lote de productos. Total hasta ahora: {len(products)}")
    
    session.close()
    logging.info(f"Total de productos obtenidos: {len(products)}")
    return products

# Función para obtener niveles de inventario desde Shopify
def fetch_inventory_levels(api_key, password, store_name, inventory_item_ids, location_ids, api_version='2023-07'):
    """
    Obtiene los niveles de inventario para una lista de inventory_item_ids y location_ids,
    retornando un diccionario de {inventory_item_id: {location_id: available, ...}, ...}.
    """
    if not inventory_item_ids:
        logging.warning("No hay inventory_item_ids para obtener niveles de inventario.")
        return {}

    base_url = f"https://{store_name}/admin/api/{api_version}/inventory_levels.json"
    headers = get_auth_headers(api_key, password)
    inventory_dict = {}
    batch_size = 25  # Tamaño de lote reducido
    total_batches = ceil(len(inventory_item_ids) / batch_size)
    session = requests.Session()
    rate_limiter = RateLimiter(max_calls=4, period=1)  # Ajustar para 4 llamadas por segundo
    max_retries = 5
    backoff_factor = 2
    max_wait_time = 60

    for i in range(total_batches):
        batch_ids = inventory_item_ids[i * batch_size: (i + 1) * batch_size]
        params = {
            'inventory_item_ids': ','.join(map(str, batch_ids)),
            'location_ids': ','.join(map(str, location_ids))
        }

        for attempt in range(max_retries):
            try:
                rate_limiter.wait()
                response = session.get(base_url, headers=headers, params=params)
                log_api_call(response)
                if response.status_code == 200:
                    data = response.json()
                    inventory_levels = data.get('inventory_levels', [])
                    if not inventory_levels:
                        logging.warning(f"No se encontraron niveles de inventario para el lote {i+1}/{total_batches}.")
                    for level in inventory_levels:
                        inventory_item_id = level['inventory_item_id']
                        location_id = level['location_id']
                        available = level.get('available', 0)
                        if inventory_item_id not in inventory_dict:
                            inventory_dict[inventory_item_id] = {}
                        inventory_dict[inventory_item_id][location_id] = available
                        logging.debug(f"Inventory Item ID: {inventory_item_id}, Location ID: {location_id}, Available: {available}")
                    logging.info(f"Niveles de inventario obtenidos para el lote {i+1}/{total_batches}.")
                    break
                elif response.status_code == 429:
                    logging.error(f"Error al obtener niveles de inventario: {response.status_code} {response.text}")
                    if handle_rate_limiting(response, attempt, backoff_factor, max_wait_time):
                        continue
                else:
                    logging.warning(f"Error al obtener niveles de inventario (intento {attempt+1}): {response.status_code} {response.text}. Reintentando.")
                    wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                    logging.info(f"Esperando {wait_time} segundos antes de reintentar.")
                    time.sleep(wait_time)
            except requests.RequestException as e:
                wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                logging.warning(f"Error en la solicitud HTTP para inventarios (intento {attempt+1}): {e}. Reintentando después de {wait_time} segundos.")
                time.sleep(wait_time)
        else:
            logging.error(f"Máximo número de intentos alcanzado para el lote {i+1}/{total_batches}. Saliendo del fetch de inventarios.")
            continue

        logging.info(f"Procesado lote de inventarios. Total hasta ahora: {len(inventory_dict)}")
    
    session.close()
    return inventory_dict

# Función para insertar o actualizar productos, variantes e inventarios en la base de datos
def insert_or_update_products_variants_and_inventory(products, conn, locations, inventory_levels):
    cursor = conn.cursor()
    product_values = []
    variant_values = []
    log_values = []
    inventory_item_to_variant = {}
    variant_id_to_barcode = {}
    barcodes_set = set()
    duplicate_barcodes = set()

    # Filtrar ubicaciones deseadas
    desired_location_ids = [loc_id for loc_id in locations if normalize_string(locations[loc_id]) in [normalize_string(name) for name in DESIRED_LOCATIONS]]

    if not desired_location_ids:
        logging.error("No se encontraron las ubicaciones deseadas. Verifica los nombres de las ubicaciones en Shopify.")
        cursor.close()
        return

    # Recolectar inventory_item_ids y verificar unicidad de barcodes
    for product in products:
        product_id = product['id']
        title = clean_string(product.get('title', 'Unknown'))
        vendor = clean_string(product.get('vendor', 'Unknown'))
        first_variant = product.get('variants', [])[0] if product.get('variants') else {}
        price = first_variant.get('price', '0.00')
        sku = clean_string(first_variant.get('sku', 'Unknown'))
        image_url = clean_string(product.get('image', {}).get('src', 'Unknown')) if product.get('image') else 'Unknown'

        product_values.append((product_id, title, vendor, price, sku, image_url))

        for variant in product.get('variants', []):
            variant_id = variant.get('id')
            if not variant_id:
                logging.warning(f"Variante sin variant_id para el producto {product_id}")
                continue
            variant_title = clean_string(variant.get('title', 'Unknown'))
            variant_sku = clean_string(variant.get('sku', 'Unknown'))
            variant_price = variant.get('price', '0.00')
            variant_barcode_original = variant.get('barcode', 'Unknown')
            variant_barcode = clean_string(variant_barcode_original)
            inventory_item_id = variant.get('inventory_item_id')  # No usaremos esta variable para la base de datos

            # Validar el barcode
            if not validate_barcode(variant_barcode):
                logging.warning(f"Barcode inválido para la variante ID {variant_id}: '{variant_barcode}'. Se establecerá como 'Unknown'.")
                variant_barcode = 'Unknown'

            # Comprobar si el barcode está duplicado
            if variant_barcode in barcodes_set:
                duplicate_barcodes.add(variant_barcode)
                logging.warning(f"Barcode duplicado para la variante ID {variant_id}: '{variant_barcode}'. Se establecerá como 'Unknown'.")
                variant_barcode = 'Unknown'
            elif variant_barcode != 'Unknown':
                barcodes_set.add(variant_barcode)

            # Mapear inventory_item_id a variant_id (para uso interno)
            if inventory_item_id:
                inventory_item_to_variant[inventory_item_id] = variant_id

            # Guardar el mapeo de variant_id a barcode
            variant_id_to_barcode[variant_id] = variant_barcode

            # Agregar variante a la lista para insertar/actualizar
            variant_values.append((
                variant_id,
                product_id,
                variant_title,
                variant_sku,
                variant_price,
                0,  # Inicializar stock a 0, se actualizará luego
                variant_barcode,
                None,  # location_id, se actualizará
                'Unknown'  # location_name, se actualizará
            ))

    # Insertar o actualizar productos
    sql_product = """
    INSERT INTO productos (product_id, title, vendor, price, sku, image_url)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE title=VALUES(title), vendor=VALUES(vendor), price=VALUES(price), sku=VALUES(sku), image_url=VALUES(image_url)
    """
    try:
        cursor.executemany(sql_product, product_values)
        logging.info(f"{cursor.rowcount} productos insertados o actualizados.")
    except mysql.connector.Error as err:
        logging.error(f"Error al insertar o actualizar productos: {err}")

    # Insertar o actualizar variantes
    sql_variant = """
    INSERT INTO product_variants (
        variant_id, 
        product_id, 
        title, 
        sku, 
        price, 
        stock, 
        barcode, 
        location_id,
        location_name
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        title=VALUES(title), 
        sku=VALUES(sku), 
        price=VALUES(price), 
        stock=VALUES(stock), 
        barcode=VALUES(barcode), 
        location_id=VALUES(location_id),
        location_name=VALUES(location_name)
    """
    try:
        cursor.executemany(sql_variant, variant_values)
        logging.info(f"{cursor.rowcount} variantes insertadas o actualizadas.")
    except mysql.connector.Error as err:
        logging.error(f"Error al insertar o actualizar variantes: {err}")

    # Obtener los inventory_item_ids para los niveles de inventario
    inventory_item_ids = list(inventory_item_to_variant.keys())

    # Obtener los niveles de inventario
    inventory_levels_fetched = inventory_levels  # Ya se ha obtenido fuera de esta función

    # Procesar los niveles de inventario
    for inventory_item_id, loc_dict in inventory_levels_fetched.items():
        variant_id = inventory_item_to_variant.get(inventory_item_id)
        if not variant_id:
            logging.warning(f"No se encontró variant_id para inventory_item_id {inventory_item_id}")
            continue

        variant_barcode = variant_id_to_barcode.get(variant_id, 'Unknown')

        for location_id, available in loc_dict.items():
            # Obtener el nombre de la ubicación
            location_name = locations.get(location_id, 'Unknown')

            if location_name == 'Unknown':
                logging.error(f"Location ID {location_id} no tiene un nombre asociado. Verifica las ubicaciones obtenidas.")
            else:
                logging.debug(f"Location ID {location_id} mapeado a {location_name}")

            # Obtener el stock anterior para esta variante y ubicación
            try:
                cursor.execute("""
                    SELECT stock FROM product_variants 
                    WHERE variant_id = %s AND location_id = %s
                """, (variant_id, location_id))
                result = cursor.fetchone()
                previous_stock = result[0] if result else None
            except mysql.connector.Error as err:
                logging.error(f"Error al obtener el stock anterior para variant_id {variant_id}, location_id {location_id}: {err}")
                previous_stock = None

            if previous_stock is not None and previous_stock != available:
                diferencia = available - previous_stock
                log_values.append((
                    inventory_item_id,
                    location_id,
                    previous_stock,
                    available,
                    diferencia,
                    variant_barcode,
                    variant_id,  # producto_id
                    variant_id,  # variante_id (puedes ajustar si es necesario)
                    location_name
                ))

            # Actualizar el stock en la base de datos
            try:
                cursor.execute("""
                    UPDATE product_variants
                    SET stock = %s
                    WHERE variant_id = %s AND location_id = %s
                """, (available, variant_id, location_id))
                logging.debug(f"Actualizado stock para variant_id {variant_id}, location_id {location_id} a {available}.")
            except mysql.connector.Error as err:
                logging.error(f"Error al actualizar stock para variant_id {variant_id}, location_id {location_id}: {err}")

    # Insertar logs de inventario si hay cambios
    if log_values:
        sql_log = """
        INSERT INTO LogsInventario (
            inventory_item_id, 
            location_id, 
            cantidad_anterior, 
            cantidad_nueva, 
            diferencia, 
            barcode, 
            producto_id, 
            variante_id, 
            nombre_ubicacion
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.executemany(sql_log, log_values)
            logging.info(f"{cursor.rowcount} registros de logs de inventario insertados.")
        except mysql.connector.Error as err:
            logging.error(f"Error al insertar logs de inventario: {err}")

    try:
        conn.commit()
        logging.info("Transacción de base de datos confirmada.")
    except mysql.connector.Error as err:
        logging.error(f"Error al confirmar la transacción: {err}")
        conn.rollback()
        logging.info("Transacción revertida debido a un error.")

    cursor.close()

# Función para eliminar productos archivados o borradores (si aplica)
def delete_archived_or_draft_products(products, conn, api_version='2023-07'):
    # Implementar según necesidad. Por ahora, omitido.
    pass

# Función principal para ejecutar el bucle de sincronización
def main_loop():
    """
    Bucle principal que ejecuta la sincronización cada 10 minutos.
    """
    while True:
        try:
            conn = create_db_connection()

            # Obtener el nombre de la tienda de manera robusta
            store_name = get_store_name(SHOPIFY_STORE_URL)

            # Obtener todas las ubicaciones
            locations = fetch_locations(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD, store_name, api_version='2023-07')

            # Filtrar ubicaciones deseadas
            desired_location_ids = get_desired_location_ids(locations, DESIRED_LOCATIONS)

            # Log de ubicaciones obtenidas y no reconocidas
            logging.info("Ubicaciones obtenidas desde Shopify:")
            for loc_id, loc_name in locations.items():
                logging.info(f"ID: {loc_id}, Nombre: {loc_name}")

            # Identificar y registrar ubicaciones no reconocidas
            normalized_desired_names = [normalize_string(name) for name in DESIRED_LOCATIONS]
            unrecognized_locations = {loc_id: loc_name for loc_id, loc_name in locations.items() if normalize_string(loc_name) not in normalized_desired_names}
            if unrecognized_locations:
                logging.warning("Ubicaciones no reconocidas:")
                for loc_id, loc_name in unrecognized_locations.items():
                    logging.warning(f"ID: {loc_id}, Nombre: {loc_name}, Normalizado: {normalize_string(loc_name)}")
            else:
                logging.info("Todas las ubicaciones están reconocidas y serán procesadas.")

            logging.info(f"Total ubicaciones obtenidas: {len(locations)}")
            logging.info(f"Location IDs válidos: {desired_location_ids}")

            if not desired_location_ids:
                logging.error("No se encontraron las ubicaciones deseadas. Verifica los nombres de las ubicaciones en Shopify.")
                conn.close()
                logging.info("Esperando 10 minutos para la siguiente sincronización.")
                time.sleep(600)
                continue

            # Obtener productos activos
            logging.info("Obteniendo detalles de productos de Shopify...")
            products = fetch_shopify_products(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD, store_name, api_version='2023-07')
            logging.info(f"Total productos obtenidos: {len(products)}")

            if products:
                # Obtener los inventory_item_ids para los niveles de inventario
                inventory_item_ids = [variant.get('inventory_item_id') for product in products for variant in product.get('variants', []) if variant.get('inventory_item_id')]
                inventory_item_ids = list(set(inventory_item_ids))  # Eliminar duplicados

                # Obtener los niveles de inventario
                inventory_levels = fetch_inventory_levels(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD, store_name=SHOPIFY_STORE_URL,
                                                         inventory_item_ids=inventory_item_ids,
                                                         location_ids=desired_location_ids,
                                                         api_version='2023-07')

                # Insertar o actualizar productos, variantes e inventarios
                insert_or_update_products_variants_and_inventory(products, conn, locations, inventory_levels)

                # Eliminar productos archivados o borradores si aplica
                delete_archived_or_draft_products(products, conn, api_version='2023-07')

                logging.info(f"Total de productos procesados: {len(products)}")
            else:
                logging.info("No se obtuvieron productos de Shopify.")

            conn.close()
        except Exception as e:
            logging.exception(f"Se produjo un error inesperado: {e}")

        logging.info("Esperando 10 minutos para la siguiente sincronización.")
        time.sleep(600)

# Ejecutar la función principal
if __name__ == "__main__":
    main_loop()
