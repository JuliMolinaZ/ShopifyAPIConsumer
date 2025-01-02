import requests
import mysql.connector
import base64
import time
import logging
from dotenv import load_dotenv
import os
import re
from math import ceil
from urllib.parse import urlparse, parse_qs

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,  # Configurado a INFO para omitir DEBUG
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("shopify_sync.log"),
        logging.StreamHandler()
    ]
)

# Cargar variables de entorno
load_dotenv()

# Variables de entorno
API_KEY = os.getenv("SHOPIFY_API_KEY")
API_PASSWORD = os.getenv("SHOPIFY_API_PASSWORD")
STORE_URL = os.getenv("SHOPIFY_STORE_URL")
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME"),
    'port': os.getenv("DB_PORT")
}

# Lista de nombres de ubicaciones deseadas
DESIRED_LOCATIONS = ["ONLINE", "CEDIS", "GUADALAJARA", "CANCÚN", "RAYONETA 1.0", "RAYONETA 2.0", "EXPERIENCIA", "LIVERPOOL"]

def create_db_connection():
    """
    Crea una conexión a la base de datos MySQL usando las configuraciones proporcionadas.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        logging.info("Conexión a la base de datos establecida.")
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Error al conectar a la base de datos: {err}")
        raise

def clean_string(value):
    """
    Limpia una cadena eliminando caracteres no ASCII y espacios innecesarios.
    """
    if value is None:
        return 'Unknown'
    return re.sub(r'[^\x00-\x7F]+', '', value).strip()

def fetch_locations(api_key, password, store_name):
    """
    Obtiene todas las ubicaciones de Shopify y retorna un diccionario de location_id a location_name.
    """
    base_url = f"https://{store_name}.myshopify.com/admin/api/2024-01/locations.json"
    
    credentials = f"{api_key}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {encoded_credentials}'
    }
    
    session = requests.Session()
    locations = {}
    url = base_url
    max_retries = 3 
    backoff_factor = 2
    max_wait_time = 32 
    
    while url:
        for attempt in range(max_retries):
            try:
                response = session.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    fetched_locations = data.get('locations', [])
                    for loc in fetched_locations:
                        locations[loc['id']] = loc['name']
                    logging.info(f"Obtenidas {len(fetched_locations)} ubicaciones.")
                    
                    link_header = response.headers.get('Link')
                    if link_header:
                        links = link_header.split(',')
                        url = None
                        for link in links:
                            if 'rel="next"' in link:
                                # Extraer la URL entre < y >
                                start = link.find('<') + 1
                                end = link.find('>')
                                if start > 0 and end > start:
                                    next_url = link[start:end]
                                    parsed_url = urlparse(next_url)
                                    query_params = parse_qs(parsed_url.query)
                                    if 'page_info' in query_params:
                                        next_page_info = query_params['page_info'][0]
                                        url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?page_info={next_page_info}"
                                break
                    else:
                        url = None
                    break
                elif response.status_code == 429:
                    retry_after = int(float(response.headers.get('Retry-After', 4)))
                    wait_time = min(backoff_factor ** attempt * retry_after, max_wait_time)
                    logging.debug(f"Límite de tasa excedido al obtener ubicaciones. Reintentando después de {wait_time} segundos.")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Error al obtener ubicaciones: {response.status_code} {response.text}")
                    return locations
            except requests.RequestException as e:
                wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                logging.debug(f"Error en la solicitud HTTP para ubicaciones: {e}. Reintentando después de {wait_time} segundos.")
                time.sleep(wait_time)
        else:
            logging.error("Máximo número de intentos alcanzado para obtener ubicaciones.")
            break
    
    session.close()
    logging.info(f"Total de ubicaciones obtenidas: {len(locations)}")
    return locations

def get_desired_location_ids(locations, desired_names):
    """
    Filtra las IDs de las ubicaciones deseadas basándose en sus nombres.
    """
    desired_ids = [loc_id for loc_id, name in locations.items() if name in desired_names]
    return desired_ids

def fetch_shopify_products(api_key, password, store_name):
    """
    Obtiene todos los productos activos de Shopify, manejando la paginación y límites de tasa.
    """
    base_url = f"https://{store_name}.myshopify.com/admin/api/2024-01/products.json"
    limit = 100
    products = []
    
    credentials = f"{api_key}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {encoded_credentials}'
    }
    
    url = f"{base_url}?limit={limit}&status=active"
    session = requests.Session()
    max_retries = 3 
    backoff_factor = 2
    max_wait_time = 32 

    while url:
        for attempt in range(max_retries):
            try:
                response = session.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    fetched_products = data.get('products', [])
                    products.extend(fetched_products)
                    logging.info(f"Se han obtenido {len(fetched_products)} productos.")
        
                    link_header = response.headers.get('Link')
                    if link_header:
                        links = link_header.split(',')
                        url = None
                        for link in links:
                            if 'rel="next"' in link:
                                # Extraer la URL entre < y >
                                start = link.find('<') + 1
                                end = link.find('>')
                                if start > 0 and end > start:
                                    next_url = link[start:end]
                                    parsed_url = urlparse(next_url)
                                    query_params = parse_qs(parsed_url.query)
                                    if 'page_info' in query_params:
                                        next_page_info = query_params['page_info'][0]
                                        url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?page_info={next_page_info}"
                                break
                    else:
                        url = None
                    break 
                elif response.status_code == 429:
                    retry_after = int(float(response.headers.get('Retry-After', 4)))
                    wait_time = min(backoff_factor ** attempt * retry_after, max_wait_time)
                    logging.debug(f"Límite de tasa excedido. Reintentando después de {wait_time} segundos.")
                    time.sleep(wait_time)
                elif response.status_code == 401:
                    logging.error(f"Error de autenticación: {response.status_code} {response.text}")
                    return products
                else:
                    logging.error(f"Error inesperado: {response.status_code} {response.text}")
                    return products
            except requests.RequestException as e:
                wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                logging.debug(f"Error en la solicitud HTTP: {e}. Reintentando después de {wait_time} segundos.")
                time.sleep(wait_time)
        else:
            logging.error("Máximo número de intentos alcanzado. Saliendo del fetch de productos.")
            break

    session.close()
    logging.info(f"Total de productos obtenidos: {len(products)}")
    return products

def fetch_inventory_levels(api_key, password, store_name, inventory_item_ids, location_ids):
    """
    Obtiene los niveles de inventario para una lista de inventory_item_ids y location_ids,
    manejando límites de tasa y errores 414.
    
    Retorna un diccionario:
        {
            inventory_item_id1: {location_id1: available, location_id2: available, ...},
            inventory_item_id2: {location_id1: available, ...},
            ...
        }
    """
    if not inventory_item_ids:
        logging.warning("No hay inventory_item_ids para obtener niveles de inventario.")
        return {}
    
    base_url = f"https://{store_name}.myshopify.com/admin/api/2024-01/inventory_levels.json"
    credentials = f"{api_key}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {encoded_credentials}'
    }
    
    inventory_dict = {}
    batch_size = 50  # Ajusta según las limitaciones de la API
    total_batches = ceil(len(inventory_item_ids) / batch_size)
    
    for i in range(total_batches):
        batch_ids = inventory_item_ids[i * batch_size : (i + 1) * batch_size]
        params = {
            'inventory_item_ids': ','.join(map(str, batch_ids)),
            'location_ids': ','.join(map(str, location_ids))
        }
        max_retries = 3  # Reducir el número máximo de reintentos
        backoff_factor = 2
        max_wait_time = 32  # Establecer un tiempo de espera máximo

        next_page_info = None

        while True:
            current_params = {}
            if next_page_info:
                current_params['page_info'] = next_page_info
            else:
                current_params.update(params)
            
            for attempt in range(max_retries):
                try:
                    response = requests.get(base_url, headers=headers, params=current_params)
                    if response.status_code == 200:
                        data = response.json()
                        inventory_levels = data.get('inventory_levels', [])
                        for level in inventory_levels:
                            inventory_item_id = level['inventory_item_id']
                            location_id = level['location_id']
                            available = level.get('available', 0)
                            if inventory_item_id not in inventory_dict:
                                inventory_dict[inventory_item_id] = {}
                            inventory_dict[inventory_item_id][location_id] = available
                        logging.debug(f"Niveles de inventario obtenidos para el lote {i+1}/{total_batches}.")
                        
                        link_header = response.headers.get('Link')
                        if link_header:
                            links = link_header.split(',')
                            next_page_info = None
                            for link in links:
                                if 'rel="next"' in link:
                                    # Extraer la URL entre < y >
                                    start = link.find('<') + 1
                                    end = link.find('>')
                                    if start > 0 and end > start:
                                        next_url = link[start:end]
                                        parsed_url = urlparse(next_url)
                                        query_params = parse_qs(parsed_url.query)
                                        if 'page_info' in query_params:
                                            next_page_info = query_params['page_info'][0]
                                    break
                        else:
                            next_page_info = None
                        break
                    elif response.status_code == 429:
                        retry_after = int(float(response.headers.get('Retry-After', 4)))
                        wait_time = min(backoff_factor ** attempt * retry_after, max_wait_time)
                        logging.debug(f"Límite de tasa excedido al obtener inventario. Reintentando después de {wait_time} segundos.")
                        time.sleep(wait_time)
                    elif response.status_code == 414:
                        logging.error(f"Error 414 Request-URI Too Large para el lote {i+1}/{total_batches}. Reduciendo el tamaño del lote.")
                        if batch_size > 1:
                            batch_size = max(1, batch_size // 2)
                            total_batches = ceil(len(inventory_item_ids) / batch_size)
                            logging.info(f"Nuevo tamaño de lote ajustado a {batch_size}.")
                        break
                    else:
                        logging.error(f"Error al obtener niveles de inventario: {response.status_code} {response.text}")
                        break
                except requests.RequestException as e:
                    wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                    logging.debug(f"Error en la solicitud HTTP para inventarios: {e}. Reintentando después de {wait_time} segundos.")
                    time.sleep(wait_time)
            else:
                logging.error(f"Máximo número de intentos alcanzado para el lote {i+1}/{total_batches}. Saliendo del fetch de inventarios.")
                break

            if not next_page_info:
                break  # No hay más páginas, salir del bucle

        logging.info(f"Procesado lote {i+1} de {total_batches}.")

    # Sumar inventario total por inventory_item_id
    aggregated_inventory = {}
    for item_id, locations in inventory_dict.items():
        aggregated_inventory[item_id] = sum(locations.values())

    if len(aggregated_inventory) != len(inventory_item_ids):
        logging.warning(f"Se esperaban {len(inventory_item_ids)} niveles de inventario, pero se obtuvieron {len(aggregated_inventory)}.")
    
    return aggregated_inventory

def validate_barcode(barcode):
    """
    Valida que el barcode cumpla con ciertos criterios.
    Permite códigos alfanuméricos de 8 a 13 caracteres.
    """
    if barcode == 'Unknown' or not barcode:
        return False
    # Permitir alfanuméricos de 8 a 13 caracteres
    if re.fullmatch(r'[A-Za-z0-9]{8,13}', barcode):
        return True
    return False

def insert_or_update_products_variants_and_inventory(products, conn, locations):
    """
    Inserta o actualiza los productos, variantes e inventarios en la base de datos.
    """
    cursor = conn.cursor()
    product_values = []
    variant_values = []
    log_values = []
    inventory_item_ids = set()
    barcodes_set = set()
    duplicate_barcodes = set()
    
    # Obtener las IDs de las ubicaciones deseadas
    desired_location_ids = get_desired_location_ids(locations, DESIRED_LOCATIONS)
    
    if not desired_location_ids:
        logging.error("No se encontraron las ubicaciones deseadas. Verifica los nombres de las ubicaciones en Shopify.")
        cursor.close()
        return
    
    # Recolectar inventory_item_ids y verificar unicidad de barcodes
    for product in products:
        if product.get('status') in ['archived', 'draft']:
            continue

        for variant in product.get('variants', []):
            inventory_item_id = variant.get('inventory_item_id')
            if inventory_item_id:
                inventory_item_ids.add(inventory_item_id)
            barcode = clean_string(variant.get('barcode'))
            if barcode in barcodes_set:
                duplicate_barcodes.add(barcode)
            elif barcode != 'Unknown':
                barcodes_set.add(barcode)
    
    if duplicate_barcodes:
        logging.warning(f"Códigos de barras duplicados encontrados: {duplicate_barcodes}")
        # Omitir variantes con códigos duplicados
        # Alternativamente, podrías manejar esto de otra manera según tus necesidades

    # Obtener los niveles de inventario para todos los inventory_item_ids y ubicaciones deseadas
    inventory_levels = fetch_inventory_levels(API_KEY, API_PASSWORD, STORE_URL.split('.')[0], list(inventory_item_ids), desired_location_ids)
    
    for product in products:
        if product.get('status') in ['archived', 'draft']:
            continue

        product_id = product['id']
        title = clean_string(product.get('title', 'Unknown'))
        vendor = clean_string(product.get('vendor', 'Unknown'))
        # Asumir que el precio y SKU del producto se toman de la primera variante activa
        first_active_variant = next((v for v in product.get('variants', []) if v.get('status') == 'active'), {})
        price = first_active_variant.get('price', '0.00')
        sku = clean_string(first_active_variant.get('sku', 'Unknown'))
        image_url = clean_string(product.get('image', {}).get('src', 'Unknown')) if product.get('image') else 'Unknown'

        product_values.append((product_id, title, vendor, price, sku, image_url))

        for variant in product.get('variants', []):
            variant_id = variant.get('id')
            variant_title = clean_string(variant.get('title', 'Unknown'))
            variant_sku = clean_string(variant.get('sku', 'Unknown'))
            variant_price = variant.get('price', '0.00')
            variant_barcode = clean_string(variant.get('barcode', 'Unknown'))
            inventory_item_id = variant.get('inventory_item_id')

            # Validar el barcode
            if not validate_barcode(variant_barcode):
                logging.warning(f"Barcode inválido para la variante ID {variant_id}: '{variant_barcode}'. Se establecerá como 'Unknown'.")
                variant_barcode = 'Unknown'

            # Comprobar si el barcode está duplicado
            if variant_barcode in duplicate_barcodes:
                logging.warning(f"Barcode duplicado para la variante ID {variant_id}: '{variant_barcode}'. Se establecerá como 'Unknown'.")
                variant_barcode = 'Unknown'

            # Obtener el nivel de inventario total considerando las ubicaciones deseadas
            variant_on_hand = inventory_levels.get(inventory_item_id, 0)

            # Obtener el stock anterior para registrar el log
            try:
                cursor.execute("SELECT stock FROM product_variants WHERE variant_id = %s", (variant_id,))
                result = cursor.fetchone()
                previous_stock = result[0] if result else None
            except mysql.connector.Error as err:
                logging.error(f"Error al obtener el stock anterior para variant_id {variant_id}: {err}")
                previous_stock = None

            if previous_stock is not None and previous_stock != variant_on_hand:
                diferencia = variant_on_hand - previous_stock
                # Obtener los nombres de las ubicaciones donde hay cambios
                # Dado que estamos sumando inventarios de múltiples ubicaciones, usaremos 'Multiple'
                log_values.append((
                    inventory_item_id,
                    0, 
                    previous_stock,
                    variant_on_hand,
                    diferencia,
                    variant_barcode,
                    product_id,
                    variant_id,
                    'Multiple'  
                ))

            variant_values.append((variant_id, product_id, variant_title, variant_sku, variant_price, variant_on_hand, variant_barcode, variant_on_hand))

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
    INSERT INTO product_variants (variant_id, product_id, title, sku, price, stock, barcode, onHand)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE title=VALUES(title), sku=VALUES(sku), price=VALUES(price), stock=VALUES(stock), barcode=VALUES(barcode), onHand=VALUES(onHand)
    """
    try:
        cursor.executemany(sql_variant, variant_values)
        logging.info(f"{cursor.rowcount} variantes insertadas o actualizadas.")
    except mysql.connector.Error as err:
        logging.error(f"Error al insertar o actualizar variantes: {err}")

    # Insertar logs de inventario si hay cambios
    if log_values:
        sql_log = """
        INSERT INTO LogsInventario (inventory_item_id, location_id, cantidad_anterior, cantidad_nueva, diferencia, barcode, producto_id, variante_id, nombre_ubicacion)
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

def delete_archived_or_draft_products(products, conn):
    """
    Elimina los productos que están archivados o en borrador en Shopify de la base de datos.
    """
    cursor = conn.cursor()
    archived_or_draft_products = []

    for product in products:
        if product.get('status') in ['archived', 'draft']:
            product_id = product['id']
            archived_or_draft_products.append(product_id)

    if not archived_or_draft_products:
        logging.info("No se encontraron productos archivados o en borrador para eliminar.")
        cursor.close()
        return

    try:
        # Seleccionar productos que existen en la base de datos
        format_strings = ','.join(['%s'] * len(archived_or_draft_products))
        cursor.execute(f"SELECT product_id FROM productos WHERE product_id IN ({format_strings})", tuple(archived_or_draft_products))
        existing_products = {row[0] for row in cursor.fetchall()}

        to_delete = existing_products.intersection(archived_or_draft_products)
        logging.info(f"Productos a eliminar: {to_delete}")

        if to_delete:
            # Eliminar variantes asociadas
            delete_variant_query = f"DELETE FROM product_variants WHERE product_id IN ({','.join(['%s'] * len(to_delete))})"
            cursor.execute(delete_variant_query, tuple(to_delete))
            # Eliminar productos
            delete_product_query = f"DELETE FROM productos WHERE product_id IN ({','.join(['%s'] * len(to_delete))})"
            cursor.execute(delete_product_query, tuple(to_delete))
            conn.commit()
            logging.info(f"Se eliminaron {cursor.rowcount} productos archivados o en borrador de la base de datos.")
        else:
            logging.info("No hay productos archivados o en borrador en la base de datos para eliminar.")
    except mysql.connector.Error as err:
        logging.error(f"Error al eliminar productos archivados o en borrador: {err}")
        conn.rollback()
    finally:
        cursor.close()

def main_loop():
    """
    Bucle principal que ejecuta la sincronización cada 3 minutos.
    """
    while True:
        try:
            conn = create_db_connection()
            
            store_name = STORE_URL.split('.')[0]
            
            # Obtener todas las ubicaciones
            locations = fetch_locations(API_KEY, API_PASSWORD, store_name)
            
            # Filtrar ubicaciones deseadas
            desired_location_ids = get_desired_location_ids(locations, DESIRED_LOCATIONS)
            
            # Log de ubicaciones obtenidas y no reconocidas
            logging.info("Ubicaciones obtenidas desde Shopify:")
            for loc_id, loc_name in locations.items():
                logging.info(f"ID: {loc_id}, Nombre: {loc_name}")
            
            unrecognized_locations = {loc_id: loc_name for loc_id, loc_name in locations.items() if loc_name not in DESIRED_LOCATIONS}
            if unrecognized_locations:
                logging.info("Ubicaciones no reconocidas (marcadas como 'Unknown'):")
                for loc_id, loc_name in unrecognized_locations.items():
                    logging.info(f"ID: {loc_id}, Nombre: {loc_name}")
            else:
                logging.info("Todas las ubicaciones están reconocidas y serán procesadas.")
            
            logging.info(f"Total ubicaciones obtenidas: {len(locations)}")
            logging.info(f"Location IDs válidos: {desired_location_ids}")
            
            if not desired_location_ids:
                logging.error("No se encontraron las ubicaciones deseadas. Verifica los nombres de las ubicaciones en Shopify.")
                conn.close()
                logging.info("Esperando 3 minutos para la siguiente sincronización.")
                time.sleep(180)
                continue

            # Obtener productos activos
            logging.info("Obteniendo detalles de productos de Shopify...")
            products = fetch_shopify_products(API_KEY, API_PASSWORD, store_name)
            logging.info(f"Total productos obtenidos: {len(products)}")

            if products:
                insert_or_update_products_variants_and_inventory(products, conn, locations)
                delete_archived_or_draft_products(products, conn)
                logging.info(f"Total de productos procesados: {len(products)}")
            else:
                logging.info("No se obtuvieron productos de Shopify.")
            
            conn.close()
        except Exception as e:
            logging.exception(f"Se produjo un error inesperado: {e}")
        
        logging.info("Esperando 3 minutos para la siguiente sincronización.")
        time.sleep(180)

if __name__ == "__main__":
    main_loop()




