import requests
import mysql.connector
import time
import json
from dotenv import load_dotenv
from collections import deque
from math import ceil
from config import Config
from db import Database
from utils import *
import logging
from shopify import *
from rate_limiter import *

def configure_logging(filename='app.log', level=logging.DEBUG):
    logging.basicConfig(
        filename=filename,
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

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

SHOPIFY_API_CONFIG = {
    'api_version': '2023-07',
    'max_retries': 5,
    'backoff_factor': 2,
    'max_wait_time': 60,
    'rate_limiter': {'max_calls': 4, 'period': 1},
}

configure_logging('prueba_sync.log')

# Función para obtener ubicaciones desde Shopify
def fetch_locations(api_key, password, store_name, api_version=None):

    # Si no se pasa api_version, se usa el valor por defecto del diccionario global
    if api_version is None:
        api_version = SHOPIFY_API_CONFIG['api_version']
        
    # Obtener los valores de la configuración global
    max_retries = SHOPIFY_API_CONFIG['max_retries']
    backoff_factor = SHOPIFY_API_CONFIG['backoff_factor']
    max_wait_time = SHOPIFY_API_CONFIG['max_wait_time']
    rate_limiter = RateLimiter(**SHOPIFY_API_CONFIG['rate_limiter'])  # Desempaquetar los parámetros de rate_limiter

    base_url = f"https://{store_name}/admin/api/{api_version}/locations.json"
    headers = get_auth_headers(api_key, password)
    session = requests.Session()
    locations = {}
    
    for attempt in range(max_retries):
        try:
            rate_limiter.wait()
            response = session.get(base_url, headers=headers, timeout=10)#agregue ell timeout
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
    #normalized_desired_names = set(normalize_string(name) for name in desired_names) // Si el número de ubicaciones y nombres deseados es grande, se puede optimizar usando conjuntos (set) para la comparación
    normalized_desired_names = [normalize_string(name) for name in desired_names]
    for loc_id, loc_name in locations.items():
        normalized_loc_name = normalize_string(loc_name)
        if normalized_loc_name in normalized_desired_names:
            desired_ids.append(loc_id)
    return desired_ids

# Función para obtener productos activos desde Shopify
def fetch_shopify_products(api_key, password, store_name, api_version=None):

    # Si no se pasa api_version, se usa el valor por defecto del diccionario global
    if api_version is None:
        api_version = SHOPIFY_API_CONFIG['api_version']
        
    # Obtener los valores de la configuración global
    max_retries = SHOPIFY_API_CONFIG['max_retries']
    backoff_factor = SHOPIFY_API_CONFIG['backoff_factor']
    max_wait_time = SHOPIFY_API_CONFIG['max_wait_time']
    rate_limiter = RateLimiter(**SHOPIFY_API_CONFIG['rate_limiter'])  # Desempaquetar los parámetros de rate_limiter

    base_url = f"https://{store_name}/admin/api/{api_version}/products.json"
    headers = get_auth_headers(api_key, password)
    session = requests.Session()
    products = []
    limit = 250  # Máximo permitido por Shopify
    url = f"{base_url}?limit={limit}&status=active"

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
def fetch_inventory_levels(api_key, password, store_name, inventory_item_ids, location_ids, api_version=None):
    """
    Obtiene los niveles de inventario para una lista de inventory_item_ids y location_ids,
    retornando un diccionario de {inventory_item_id: {location_id: available, ...}, ...}.
    """

    if not inventory_item_ids:
        logging.warning("No hay inventory_item_ids para obtener niveles de inventario.")
        return {}
    
    # Si no se pasa api_version, se usa el valor por defecto del diccionario global
    if api_version is None:
        api_version = SHOPIFY_API_CONFIG['api_version']
        
    # Obtener los valores de la configuración global
    max_retries = SHOPIFY_API_CONFIG['max_retries']
    backoff_factor = SHOPIFY_API_CONFIG['backoff_factor']
    max_wait_time = SHOPIFY_API_CONFIG['max_wait_time']
    rate_limiter = RateLimiter(**SHOPIFY_API_CONFIG['rate_limiter'])  # Desempaquetar los parámetros de rate_limiter

    base_url = f"https://{store_name}/admin/api/{api_version}/inventory_levels.json"
    headers = get_auth_headers(api_key, password)
    inventory_dict = {}
    batch_size = 25  # Tamaño de lote reducido
    total_batches = ceil(len(inventory_item_ids) / batch_size)
    session = requests.Session()

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


def insert_or_update_products_variants_and_inventory(products, conn, locations, inventory_levels):
    cursor = conn.cursor()
    try:
        desired_location_ids = filter_desired_locations(locations)
        if not desired_location_ids:
            logging.error("No se encontraron las ubicaciones deseadas. Verifica los nombres de las ubicaciones en Shopify.")
            return

        product_values, variant_values, mappings = process_products(products)
        print("antes de insertar")
        insert_or_update_products(cursor, product_values)
        insert_or_update_variants(cursor, variant_values)
        update_inventory(cursor, inventory_levels, mappings, locations)

        conn.commit()
        logging.info("Transacción de base de datos confirmada.")
    except mysql.connector.Error as err:
        logging.error(f"Error durante la transacción: {err}")
        conn.rollback()
        logging.info("Transacción revertida debido a un error.")
    finally:
        cursor.close()

def filter_desired_locations(locations):
    """Filtra las ubicaciones deseadas según DESIRED_LOCATIONS."""
    return [loc_id for loc_id in locations if normalize_string(locations[loc_id]) in [normalize_string(name) for name in DESIRED_LOCATIONS]]

def process_products(products):
    """Procesa los productos y variantes, y devuelve los valores para inserción/actualización y mapeos internos."""
    product_values = []
    variant_values = []
    inventory_item_to_variant = {}
    variant_id_to_barcode = {}
    barcodes_set = set()
    duplicate_barcodes = set()

    for product in products:
        product_id, title, vendor, price, sku, image_url = extract_product_info(product)
        product_values.append((product_id, title, vendor, price, sku, image_url))

        for variant in product.get('variants', []):
            variant_info = extract_variant_info(variant, product_id, barcodes_set, duplicate_barcodes)
            if variant_info:
                variant_values.append(variant_info["values"])
                inventory_item_to_variant[variant_info["inventory_item_id"]] = variant_info["variant_id"]
                variant_id_to_barcode[variant_info["variant_id"]] = variant_info["barcode"]

    return product_values, variant_values, {"inventory_item_to_variant": inventory_item_to_variant, "variant_id_to_barcode": variant_id_to_barcode}

def extract_product_info(product):
    """Extrae y limpia la información de un producto."""
    product_id = product['id']
    title = clean_string(product.get('title', 'Unknown'))
    vendor = clean_string(product.get('vendor', 'Unknown'))
    first_variant = product.get('variants', [])[0] if product.get('variants') else {}
    price = first_variant.get('price', '0.00')
    sku = clean_string(first_variant.get('sku', 'Unknown'))
    image_url = clean_string(product.get('image', {}).get('src', 'Unknown')) if product.get('image') else 'Unknown'
    return product_id, title, vendor, price, sku, image_url

def extract_variant_info(variant, product_id, barcodes_set, duplicate_barcodes):
    """Extrae y limpia la información de una variante."""
    variant_id = variant.get('id')
    if not variant_id:
        logging.warning(f"Variante sin variant_id para el producto {product_id}")
        return None

    variant_title = clean_string(variant.get('title', 'Unknown'))
    variant_sku = clean_string(variant.get('sku', 'Unknown'))
    variant_price = variant.get('price', '0.00')
    variant_barcode_original = variant.get('barcode', 'Unknown')
    variant_barcode = clean_string(variant_barcode_original)
    inventory_item_id = variant.get('inventory_item_id')

    if not validate_barcode(variant_barcode) or variant_barcode in duplicate_barcodes:
        logging.warning(f"Barcode inválido o duplicado para la variante ID {variant_id}. Se establecerá como 'Unknown'.")
        variant_barcode = 'Unknown'
    elif variant_barcode != 'Unknown':
        barcodes_set.add(variant_barcode)

    return {
        "values": (
            variant_id, product_id, variant_title, variant_sku, variant_price, 0, variant_barcode, None, 'Unknown'
        ),
        "inventory_item_id": inventory_item_id,
        "variant_id": variant_id,
        "barcode": variant_barcode
    }

def insert_or_update_products(cursor, product_values):
    """Inserta o actualiza los productos en la base de datos."""
    sql_product = """
    INSERT INTO productos (product_id, title, vendor, price, sku, image_url)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE title=VALUES(title), vendor=VALUES(vendor), price=VALUES(price), sku=VALUES(sku), image_url=VALUES(image_url)
    """
    cursor.executemany(sql_product, product_values)
    logging.info(f"{cursor.rowcount} productos insertados o actualizados.")

def insert_or_update_variants(cursor, variant_values):
    """Inserta o actualiza las variantes en la base de datos."""
    sql_variant = """
    INSERT INTO product_variants (
        variant_id, product_id, title, sku, price, stock, barcode, location_id, location_name
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        title=VALUES(title), sku=VALUES(sku), price=VALUES(price), stock=VALUES(stock), barcode=VALUES(barcode), location_id=VALUES(location_id), location_name=VALUES(location_name)
    """
    cursor.executemany(sql_variant, variant_values)
    logging.info(f"{cursor.rowcount} variantes insertadas o actualizadas.")

def update_inventory(cursor, inventory_levels, mappings, locations):
    """Actualiza los niveles de inventario y registra los cambios."""
    inventory_item_to_variant = mappings["inventory_item_to_variant"]
    variant_id_to_barcode = mappings["variant_id_to_barcode"]
    log_values = []

    for inventory_item_id, loc_dict in inventory_levels.items():
        variant_id = inventory_item_to_variant.get(inventory_item_id)
        if not variant_id:
            logging.warning(f"No se encontró variant_id para inventory_item_id {inventory_item_id}")
            continue

        for location_id, available in loc_dict.items():
            location_name = locations.get(location_id, 'Unknown')
            if location_name == 'Unknown':
                logging.error(f"Location ID {location_id} no tiene un nombre asociado.")

            log_inventory_changes(cursor, log_values, inventory_item_id, location_id, available, variant_id, location_name, variant_id_to_barcode)

    if log_values:
        insert_inventory_logs(cursor, log_values)

def log_inventory_changes(cursor, log_values, inventory_item_id, location_id, available, variant_id, location_name, variant_id_to_barcode):
    """Registra los cambios en el inventario si son necesarios."""
    try:
        cursor.execute("SELECT stock FROM product_variants WHERE variant_id = %s AND location_id = %s", (variant_id, location_id))
        result = cursor.fetchone()
        previous_stock = result[0] if result else None
    except mysql.connector.Error as err:
        logging.error(f"Error al obtener el stock anterior para variant_id {variant_id}, location_id {location_id}: {err}")
        return

    if previous_stock is not None and previous_stock != available:
        diferencia = available - previous_stock
        log_values.append((
            inventory_item_id, location_id, previous_stock, available, diferencia, 
            variant_id_to_barcode.get(variant_id, 'Unknown'), variant_id, variant_id, location_name
        ))

    try:
        cursor.execute("""
            UPDATE product_variants
            SET stock = %s
            WHERE variant_id = %s AND location_id = %s
        """, (available, variant_id, location_id))
        logging.debug(f"Actualizado stock para variant_id {variant_id}, location_id {location_id} a {available}.")
    except mysql.connector.Error as err:
        logging.error(f"Error al actualizar stock para variant_id {variant_id}, location_id {location_id}: {err}")

def insert_inventory_logs(cursor, log_values):
    """Inserta los registros de cambios de inventario en la base de datos."""
    sql_log = """
    INSERT INTO LogsInventario (
        inventory_item_id, location_id, cantidad_anterior, cantidad_nueva, diferencia, barcode, producto_id, variante_id, nombre_ubicacion
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(sql_log, log_values)
    logging.info(f"{cursor.rowcount} registros de logs de inventario insertados.")



db_config=Config()

locations = fetch_locations(db_config.SHOPIFY_API_KEY, db_config.SHOPIFY_API_PASSWORD, db_config.SHOPIFY_STORE_URL, SHOPIFY_API_CONFIG['api_version'])

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
    #conn.close()
    logging.info("Esperando 10 minutos para la siguiente sincronización.")
    #time.sleep(600)
    #continue

# Obtener productos activos
logging.info("Obteniendo detalles de productos de Shopify...")
products = fetch_shopify_products(db_config.SHOPIFY_API_KEY, db_config.SHOPIFY_API_PASSWORD, db_config.SHOPIFY_STORE_URL, SHOPIFY_API_CONFIG['api_version'])
logging.info(f"Total productos obtenidos: {len(products)}")

if products:

    # Obtener los inventory_item_ids para los niveles de inventario
    inventory_item_ids = [variant.get('inventory_item_id') for product in products for variant in product.get('variants', []) if variant.get('inventory_item_id')]
    inventory_item_ids = list(set(inventory_item_ids))  # Eliminar duplicados
    print(f"Total items ids: {len(inventory_item_ids)}")

    # Obtener los niveles de inventario
    inventory_levels = fetch_inventory_levels(db_config.SHOPIFY_API_KEY, db_config.SHOPIFY_API_PASSWORD, db_config.SHOPIFY_STORE_URL,
                                                            inventory_item_ids,
                                                            desired_location_ids,
                                                            SHOPIFY_API_CONFIG['api_version'])
print(f"Total productos obtenidos: {len(products)}")
with Database(db_config.DB_HOST, db_config.DB_PORT, db_config.DB_USER, db_config.DB_PASSWORD, db_config.DB_NAME) as conn:
   insert_or_update_products_variants_and_inventory(products, conn, locations, inventory_levels)

conn.close()

print("FIN PROGRAMA")

# while True:

#     try:
#         with Database(db_config.DB_HOST, db_config.DB_PORT, db_config.DB_USER, db_config.DB_PASSWORD, db_config.DB_NAME) as conn:
#             locations = fetch_locations(db_config.SHOPIFY_API_KEY, db_config.SHOPIFY_API_PASSWORD, db_config.SHOPIFY_STORE_URL, SHOPIFY_API_CONFIG['api_version'])

#             desired_location_ids = get_desired_location_ids(locations, DESIRED_LOCATIONS)

#             # Log de ubicaciones obtenidas y no reconocidas
#             logging.info("Ubicaciones obtenidas desde Shopify:")
#             for loc_id, loc_name in locations.items():
#                 logging.info(f"ID: {loc_id}, Nombre: {loc_name}")

#             # Identificar y registrar ubicaciones no reconocidas
#             normalized_desired_names = [normalize_string(name) for name in DESIRED_LOCATIONS]
#             unrecognized_locations = {loc_id: loc_name for loc_id, loc_name in locations.items() if normalize_string(loc_name) not in normalized_desired_names}
#             if unrecognized_locations:
#                 logging.warning("Ubicaciones no reconocidas:")
#                 for loc_id, loc_name in unrecognized_locations.items():
#                         logging.warning(f"ID: {loc_id}, Nombre: {loc_name}, Normalizado: {normalize_string(loc_name)}")
#             else:
#                 logging.info("Todas las ubicaciones están reconocidas y serán procesadas.")

#             logging.info(f"Total ubicaciones obtenidas: {len(locations)}")
#             logging.info(f"Location IDs válidos: {desired_location_ids}")

#             if not desired_location_ids:
#                 logging.error("No se encontraron las ubicaciones deseadas. Verifica los nombres de las ubicaciones en Shopify.")
#                 conn.close()
#                 logging.info("Esperando 10 minutos para la siguiente sincronización.")
#                 time.sleep(600)
#                 continue

#             # Obtener productos activos
#             logging.info("Obteniendo detalles de productos de Shopify...")
#             products = fetch_shopify_products(db_config.SHOPIFY_API_KEY, db_config.SHOPIFY_API_PASSWORD, db_config.SHOPIFY_STORE_URL, SHOPIFY_API_CONFIG['api_version'])
#             logging.info(f"Total productos obtenidos: {len(products)}")

#             if products:

#                     # Obtener los inventory_item_ids para los niveles de inventario
#                     inventory_item_ids = [variant.get('inventory_item_id') for product in products for variant in product.get('variants', []) if variant.get('inventory_item_id')]
#                     inventory_item_ids = list(set(inventory_item_ids))  # Eliminar duplicados
#                     print(f"Total items ids: {len(inventory_item_ids)}")

#                     # Obtener los niveles de inventario
#                     inventory_levels = fetch_inventory_levels(db_config.SHOPIFY_API_KEY, db_config.SHOPIFY_API_PASSWORD, db_config.SHOPIFY_STORE_URL,
#                                                             inventory_item_ids,
#                                                             desired_location_ids,
#                                                             SHOPIFY_API_CONFIG['api_version'])
                    
#                     insert_or_update_products_variants_and_inventory(products, conn, locations, inventory_levels)

#                     logging.info(f"Total de productos procesados: {len(products)}")
#             else:
#                     logging.info("No se obtuvieron productos de Shopify.")

#             conn.close()
#     except Exception as e:
#         logging.exception(f"Se produjo un error inesperado: {e}")

#     logging.info("Esperando 10 minutos para la siguiente sincronización.")
#     time.sleep(600)


    

        