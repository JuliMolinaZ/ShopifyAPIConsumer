import requests
import mysql.connector
import time
import json
from collections import defaultdict
from math import ceil
import logging

# Ajusta estos imports a tus archivos locales:
from config import Config
from db import Database
from utils import *          
from shopify import *        
from rate_limiter import *  

def configure_logging(filename='app.log', level=logging.INFO):
    logging.basicConfig(
        filename=filename,
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

start_time = time.time()

DESIRED_LOCATIONS = [
    'CANCÚN',
    'CEDIS',
    'EXPERIENCIA',
    'GUADALAJARA',
    'LIVERPOOL',
    'ONLINE',
    'RAYONETA 1.0',
    'RAYONETA 2.0',
    'FOTOS'
]

SHOPIFY_API_CONFIG = {
    'api_version': '2023-07',
    'max_retries': 5,
    'backoff_factor': 2,
    'max_wait_time': 60,
    'rate_limiter': {'max_calls': 2, 'period': 1},
}

configure_logging('prueba_sync.log', level=logging.INFO)

# --------------------------------------------------------------------
# FUNCIONES PARA OBTENER UBICACIONES, PRODUCTOS, INVENTARIO, ETC.
# --------------------------------------------------------------------

def fetch_locations(api_key, password, store_name, api_version=None):
    """
    Descarga la lista de ubicaciones desde Shopify (locations.json) y
    retorna un dict { location_id: location_name }.
    """
    if api_version is None:
        api_version = SHOPIFY_API_CONFIG['api_version']
    max_retries = SHOPIFY_API_CONFIG['max_retries']
    backoff_factor = SHOPIFY_API_CONFIG['backoff_factor']
    max_wait_time = SHOPIFY_API_CONFIG['max_wait_time']
    rate_limiter = RateLimiter(**SHOPIFY_API_CONFIG['rate_limiter'])

    base_url = f"https://{store_name}/admin/api/{api_version}/locations.json"
    headers = get_auth_headers(api_key, password)
    session = requests.Session()
    locations = {}
    
    for attempt in range(max_retries):
        try:
            rate_limiter.wait()
            response = session.get(base_url, headers=headers, timeout=10)
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
            logging.warning(f"Error HTTP para ubicaciones (intento {attempt+1}): {e}. Reintentando en {wait_time}s.")
            time.sleep(wait_time)

    logging.error("Máximo número de intentos alcanzado. No se obtuvieron ubicaciones.")
    return locations


def get_desired_location_ids(locations, desired_names):
    """
    Retorna la lista de location_id cuyo nombre (normalizado) coincide 
    con DESIRED_LOCATIONS (también normalizado).
    """
    desired_ids = []
    normalized_desired_names = [normalize_string(name) for name in desired_names]
    for loc_id, loc_name in locations.items():
        normalized_loc_name = normalize_string(loc_name)
        if normalized_loc_name in normalized_desired_names:
            desired_ids.append(loc_id)
    return desired_ids


def fetch_shopify_products(api_key, password, store_name, api_version=None):
    """
    Descarga todos los productos (paginando) desde Shopify.
    Si quieres traer borradores y archivados, quita '?status=active'
    o usa '?published_status=any' en la URL.
    """
    if api_version is None:
        api_version = SHOPIFY_API_CONFIG['api_version']
    max_retries = SHOPIFY_API_CONFIG['max_retries']
    backoff_factor = SHOPIFY_API_CONFIG['backoff_factor']
    max_wait_time = SHOPIFY_API_CONFIG['max_wait_time']
    rate_limiter = RateLimiter(**SHOPIFY_API_CONFIG['rate_limiter'])

    base_url = f"https://{store_name}/admin/api/{api_version}/products.json"
    headers = get_auth_headers(api_key, password)
    session = requests.Session()
    products = []
    limit = 250
    url = f"{base_url}?limit={limit}"  # Ajusta si quieres status=any

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
                    link_header = response.headers.get('Link')
                    if link_header:
                        links = link_header.split(',')
                        url = None
                        for link in links:
                            if 'rel="next"' in link:
                                start = link.find('<') + 1
                                end = link.find('>')
                                if start > 0 and end > start:
                                    url = link[start:end]
                                break
                    else:
                        url = None
                    break
                elif response.status_code == 429:
                    logging.error(f"Error 429 al obtener productos: {response.text}")
                    if handle_rate_limiting(response, attempt, backoff_factor, max_wait_time):
                        continue
                else:
                    logging.warning(f"Error inesperado (intento {attempt+1}): {response.status_code} {response.text}")
                    wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                    logging.info(f"Esperando {wait_time}s antes de reintentar.")
                    time.sleep(wait_time)
            except requests.RequestException as e:
                wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                logging.warning(f"Error HTTP (intento {attempt+1}): {e}. Reintentando en {wait_time}s.")
                time.sleep(wait_time)
        else:
            logging.error("Máximo número de intentos alcanzado al obtener productos. Terminando.")
            break

        logging.info(f"Procesado lote de productos. Total hasta ahora: {len(products)}")
    
    session.close()
    logging.info(f"Total de productos obtenidos: {len(products)}")
    return products


def fetch_inventory_levels(api_key, password, store_name, inventory_item_ids, location_ids, api_version=None):
    """
    Dado un conjunto de inventory_item_ids y location_ids, 
    llama a /inventory_levels.json en lotes para cada subset de 25 items.
    Retorna un dict: { inventory_item_id: { location_id: available, ... }, ... }
    """
    if not inventory_item_ids:
        logging.warning("No hay inventory_item_ids para obtener niveles de inventario.")
        return {}
    if api_version is None:
        api_version = SHOPIFY_API_CONFIG['api_version']
    max_retries = SHOPIFY_API_CONFIG['max_retries']
    backoff_factor = SHOPIFY_API_CONFIG['backoff_factor']
    max_wait_time = SHOPIFY_API_CONFIG['max_wait_time']
    rate_limiter = RateLimiter(**SHOPIFY_API_CONFIG['rate_limiter'])

    base_url = f"https://{store_name}/admin/api/{api_version}/inventory_levels.json"
    headers = get_auth_headers(api_key, password)
    inventory_dict = {}
    batch_size = 25
    total_batches = ceil(len(inventory_item_ids) / batch_size)
    session = requests.Session()

    for i in range(total_batches):
        batch_ids = inventory_item_ids[i * batch_size : (i + 1) * batch_size]
        params = {
            'inventory_item_ids': ','.join(map(str, batch_ids)),
            'location_ids': ','.join(map(str, location_ids))
        }
        logging.info(f"Procesando lote {i+1}/{total_batches} de inventarios (items: {len(batch_ids)})")

        for attempt in range(max_retries):
            try:
                rate_limiter.wait()
                response = session.get(base_url, headers=headers, params=params, timeout=10)
                log_api_call(response)
                if response.status_code == 200:
                    data = response.json()
                    inventory_levels = data.get('inventory_levels', [])
                    if not inventory_levels:
                        logging.warning(f"No se encontraron niveles de inventario para el lote {i+1}.")
                    for level in inventory_levels:
                        inventory_item_id = level['inventory_item_id']
                        location_id = level['location_id']
                        raw_available = level.get('available', 0)
                        available = int(raw_available) if raw_available else 0

                        if inventory_item_id not in inventory_dict:
                            inventory_dict[inventory_item_id] = {}
                        inventory_dict[inventory_item_id][location_id] = available

                        logging.debug(f"ItemID: {inventory_item_id}, LocID: {location_id}, Avail: {available}")
                    logging.info(f"Niveles de inventario obtenidos para el lote {i+1}.")
                    break
                elif response.status_code == 429:
                    logging.error(f"429 al obtener inventario: {response.text}")
                    if handle_rate_limiting(response, attempt, backoff_factor, max_wait_time):
                        continue
                else:
                    logging.warning(f"Error {response.status_code} (intento {attempt+1}): {response.text}")
                    wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                    logging.info(f"Esperando {wait_time}s antes de reintentar.")
                    time.sleep(wait_time)
            except requests.RequestException as e:
                wait_time = min(backoff_factor ** attempt * 5, max_wait_time)
                logging.warning(f"Error HTTP (lote {i+1}, intento {attempt+1}): {e}. Esperando {wait_time}s.")
                time.sleep(wait_time)
        else:
            logging.error(f"Máximo intentos para el lote {i+1}. Saliendo.")
            continue

        logging.info(f"Procesado lote {i+1} de inventarios. Total items dict={len(inventory_dict)}")

    session.close()
    return inventory_dict


# --------------------------------------------------------------------
# INSERT/UPDATE EN DB
# --------------------------------------------------------------------

def insert_or_update_products_variants_and_inventory(products, conn, locations, inventory_levels):
    """
    1) Insert/Update tabla `productos`
    2) Insert/Update tabla `product_variants`
    3) Insert/Update tabla `inventory`
    """
    cursor = conn.cursor()
    try:
        # desired_location_ids = filter_desired_locations(locations)
        desired_location_ids = list(locations.keys())  # <--- prueba: tomar TODAS
        if not desired_location_ids:
            logging.error("No se encontraron las ubicaciones deseadas. Verifica DESIRED_LOCATIONS y los nombres reales.")
            return None

        # B) Procesar productos para armar product_values, variant_values y mappings
        product_values, variant_values, mappings = process_products(products)
        logging.info(f"Productos a insertar/actualizar: {len(product_values)}; Variantes: {len(variant_values)}")
        print("Antes de insertar en DB...")

        # C) Insertar/actualizar en 'productos'
        insert_or_update_products(cursor, product_values)
        # D) Insertar/actualizar en 'product_variants'
        insert_or_update_variants(cursor, variant_values)
        # E) Insertar/actualizar en 'inventory'
        upsert_inventory(cursor, inventory_levels, mappings, locations)

        conn.commit()
        logging.info("Transacción confirmada.")
        return mappings  # Por si deseas usarlo en compare_inventories_and_log
    except mysql.connector.Error as err:
        logging.error(f"Error durante la transacción: {err}")
        conn.rollback()
        logging.info("Transacción revertida.")
        return None
    finally:
        cursor.close()


def filter_desired_locations(locations):
    """
    Retorna los loc_id que, al normalizar su nombre, coincidan
    con DESIRED_LOCATIONS (también normalizadas).
    """
    return [
        loc_id
        for loc_id in locations
        if normalize_string(locations[loc_id]) in [normalize_string(name) for name in DESIRED_LOCATIONS]
    ]


def process_products(products):
    """
    Genera:
     - product_values -> lista de tuplas para INSERT en 'productos'
     - variant_values -> lista de tuplas para INSERT en 'product_variants'
     - mappings -> dict con:
         inventory_item_to_variant => {inventory_item_id: variant_id}
         variant_id_to_barcode => {variant_id: barcode}
    """
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
                inv_item_id = variant_info["inventory_item_id"]
                var_id = variant_info["variant_id"]
                # Mapeo item->variant
                inventory_item_to_variant[inv_item_id] = var_id
                # Mapeo variant->barcode
                variant_id_to_barcode[var_id] = variant_info["barcode"]

    return product_values, variant_values, {
        "inventory_item_to_variant": inventory_item_to_variant,
        "variant_id_to_barcode": variant_id_to_barcode
    }


def extract_product_info(product):
    """
    Toma un 'product' de Shopify y extrae info básica: (id, title, vendor, price, sku, image_url).
    """
    product_id = product['id']
    title = clean_string(product.get('title', 'Unknown'))
    vendor = clean_string(product.get('vendor', 'Unknown'))
    first_variant = product.get('variants', [])[0] if product.get('variants') else {}
    price = first_variant.get('price', '0.00')
    sku = clean_string(first_variant.get('sku', 'Unknown'))
    image_url = clean_string(product.get('image', {}).get('src', 'Unknown')) if product.get('image') else 'Unknown'
    return product_id, title, vendor, price, sku, image_url


def extract_variant_info(variant, product_id, barcodes_set, duplicate_barcodes):
    """
    Toma un 'variant' de Shopify, verifica su 'barcode', etc.
    Retorna un dict con:
      {
        "values": (variant_id, product_id, variant_title, ...),
        "inventory_item_id": ...,
        "variant_id": ...,
        "barcode": ...
      }
    """
    variant_id = variant.get('id')
    if not variant_id:
        logging.warning(f"Variante sin variant_id en product_id={product_id}")
        return None

    variant_title = clean_string(variant.get('title', 'Unknown'))
    variant_sku = clean_string(variant.get('sku', 'Unknown'))
    variant_price = variant.get('price', '0.00')
    variant_barcode_original = variant.get('barcode', 'Unknown')
    variant_barcode = clean_string(variant_barcode_original)
    inventory_item_id = variant.get('inventory_item_id')

    # Validar barcode, evitar duplicados
    if not validate_barcode(variant_barcode) or variant_barcode in duplicate_barcodes:
        logging.warning(f"Barcode inválido/duplicado para la variante {variant_id}. Se usará 'Unknown'.")
        variant_barcode = 'Unknown'
    elif variant_barcode != 'Unknown':
        barcodes_set.add(variant_barcode)

    return {
        "values": (
            variant_id,      # variant_id
            product_id,      # product_id
            variant_title,   # title
            variant_sku,     # sku
            variant_price,   # price
            0,               # stock (lo gestionamos en la tabla inventory)
            variant_barcode, # barcode
            'Unknown'        # location_name (si lo tienes en product_variants)
        ),
        "inventory_item_id": inventory_item_id,
        "variant_id": variant_id,
        "barcode": variant_barcode
    }


def insert_or_update_products(cursor, product_values):
    """
    Inserta/actualiza en la tabla 'productos'.
    """
    sql_product = """
    INSERT INTO productos (product_id, title, vendor, price, sku, image_url)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        title=VALUES(title),
        vendor=VALUES(vendor),
        price=VALUES(price),
        sku=VALUES(sku),
        image_url=VALUES(image_url)
    """
    cursor.executemany(sql_product, product_values)
    logging.info(f"{cursor.rowcount} productos insertados/actualizados.")


def insert_or_update_variants(cursor, variant_values):
    """
    Inserta/actualiza en la tabla 'product_variants'.
    """
    sql_variant = """
    INSERT INTO product_variants (
        variant_id, product_id, title, sku, price, stock, barcode, location_name
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        title=VALUES(title),
        sku=VALUES(sku),
        price=VALUES(price),
        stock=VALUES(stock),
        barcode=VALUES(barcode),
        location_name=VALUES(location_name)
    """
    cursor.executemany(sql_variant, variant_values)
    logging.info(f"{cursor.rowcount} variantes insertadas/actualizadas.")


def upsert_inventory(cursor, inventory_levels, mappings, locations):
    """
    Inserta/actualiza en la tabla 'inventory' con la PK compuesta 
    (variant_id, location_id) o un ID autoincrement + UNIQUE(variant_id, location_id).

    inventory_levels: { inventory_item_id: { location_id: available } }
    mappings["inventory_item_to_variant"]: { inventory_item_id: variant_id }
    mappings["variant_id_to_barcode"]: { variant_id: barcode }

    """
    inventory_item_to_variant = mappings["inventory_item_to_variant"]
    variant_id_to_barcode = mappings["variant_id_to_barcode"]
    update_values = []

    for inventory_item_id, loc_dict in inventory_levels.items():
        variant_id = inventory_item_to_variant.get(inventory_item_id)
        if not variant_id:
            logging.warning(f"No se encontró variant_id para inventory_item_id={inventory_item_id}")
            continue

        barcode = variant_id_to_barcode.get(variant_id, "Unknown")

        for location_id, available in loc_dict.items():
            if available is None:
                available = 0
            
            location_name = locations.get(location_id, "Unknown")

            # (variant_id, location_id, location_name, barcode, stock)
            update_values.append((variant_id, location_id, location_name, barcode, available))

    if update_values:
        sql_update = """
            INSERT INTO inventory (
                variant_id, location_id, location_name, barcode, stock
            )
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                location_name = VALUES(location_name),
                barcode = VALUES(barcode),
                stock = VALUES(stock)
        """
        cursor.executemany(sql_update, update_values)
        logging.info(f"{cursor.rowcount} registros en `inventory` upsert.")


# --------------------------------------------------------------------
#  FUNCIÓN: comparar inventarios y guardar logs de diferencias
# --------------------------------------------------------------------
def compare_inventories_and_log(conn, inventory_levels, mappings, locations):
    """
    Compara la tabla `inventory` en DB vs. `inventory_levels` de Shopify
    y registra las diferencias en `inventory_logs`.
    """
    cursor = conn.cursor(dictionary=True)

    # 1) Construir un dict con stocks en DB: { (variant_id, location_id): stock_db }
    db_stocks = {}
    cursor.execute("SELECT variant_id, location_id, stock FROM inventory")
    rows = cursor.fetchall()
    for row in rows:
        key = (row["variant_id"], row["location_id"])
        db_stocks[key] = row["stock"]

    inventory_item_to_variant = mappings["inventory_item_to_variant"]

    # Lista de diffs
    diff_logs = []

    # 2) Recorrer inventory_levels
    for inventory_item_id, loc_dict in inventory_levels.items():
        variant_id = inventory_item_to_variant.get(inventory_item_id)
        if not variant_id:
            continue

        for location_id, stock_shopify in loc_dict.items():
            if stock_shopify is None:
                stock_shopify = 0
            key = (variant_id, location_id)
            stock_db = db_stocks.get(key)

            if stock_db is None:
                # No existe en la tabla 'inventory'
                diff_logs.append({
                    "variant_id": variant_id,
                    "location_id": location_id,
                    "location_name": locations.get(location_id, "Unknown"),
                    "stock_shopify": stock_shopify,
                    "stock_db": 0
                })
            else:
                if stock_db != stock_shopify:
                    diff_logs.append({
                        "variant_id": variant_id,
                        "location_id": location_id,
                        "location_name": locations.get(location_id, "Unknown"),
                        "stock_shopify": stock_shopify,
                        "stock_db": stock_db
                    })

    # 3) Insertar esas diffs en la tabla logs (inventory_logs)
    if diff_logs:
        sql_log = """
            INSERT INTO inventory_logs (
                variant_id, location_id, location_name, stock_shopify, stock_db
            )
            VALUES (%s, %s, %s, %s, %s)
        """
        log_values = []
        for d in diff_logs:
            log_values.append((
                d["variant_id"],
                d["location_id"],
                d["location_name"],
                d["stock_shopify"],
                d["stock_db"]
            ))

        cursor.executemany(sql_log, log_values)
        conn.commit()

        logging.info(f"{cursor.rowcount} registros de diferencias guardados en `inventory_logs`.")
        print("\n=== D I F E R E N C I A S  D E  I N V E N T A R I O ===\n")

        # Imprimir en consola
        from collections import defaultdict
        dif_por_variant = defaultdict(list)
        for d in diff_logs:
            dif_por_variant[d["variant_id"]].append(d)
        for var_id, items in dif_por_variant.items():
            print(f"Variant ID: {var_id}")
            for x in items:
                loc_name = x["location_name"]
                sh = x["stock_shopify"]
                db = x["stock_db"]
                print(f"   Ubic: {loc_name} -> Shopify={sh}, DB={db}")
            print()
        print("=== FIN REPORTE DE DIFERENCIAS ===")
    else:
        print("No hay diferencias de inventario entre Shopify y la base de datos.")

    cursor.close()


# --------------------------------------------------------------------
# SCRIPT PRINCIPAL
# --------------------------------------------------------------------
if __name__ == "__main__":
    db_config = Config()

    # 1) Fetch ubicaciones
    locations = fetch_locations(
        db_config.SHOPIFY_API_KEY,
        db_config.SHOPIFY_API_PASSWORD,
        db_config.SHOPIFY_STORE_URL,
        SHOPIFY_API_CONFIG['api_version']
    )

    # Mostrar la normalización para depurar
    normalized_desired_names = [normalize_string(name) for name in DESIRED_LOCATIONS]
    logging.info(f"Nombres deseados normalizados: {normalized_desired_names}")

    for loc_id, loc_name in locations.items():
        logging.info(f"Ubicación - ID: {loc_id}, Nombre: '{loc_name}', Normalizado: '{normalize_string(loc_name)}'")

    # 2) Determinar location_ids filtrados
    desired_location_ids = get_desired_location_ids(locations, DESIRED_LOCATIONS)
    logging.info("Ubicaciones obtenidas desde Shopify:")
    for loc_id, loc_name in locations.items():
        logging.info(f"ID: {loc_id}, Nombre: {loc_name}")

    # Ubicaciones no reconocidas
    unrecognized_locations = {
        loc_id: loc_name
        for loc_id, loc_name in locations.items()
        if normalize_string(loc_name) not in normalized_desired_names
    }
    if unrecognized_locations:
        logging.warning("Ubicaciones no reconocidas:")
        for loc_id, loc_name in unrecognized_locations.items():
            logging.warning(f"ID: {loc_id}, Nombre: '{loc_name}', Normalizado='{normalize_string(loc_name)}'")
    else:
        logging.info("Todas las ubicaciones están reconocidas y serán procesadas.")

    logging.info(f"Total ubicaciones obtenidas: {len(locations)}")
    logging.info(f"Location IDs válidos (filtrados): {desired_location_ids}")

    if not desired_location_ids:
        logging.error("No se encontraron las ubicaciones deseadas. Saliendo.")
        exit()

    # 3) Fetch productos
    logging.info("Obteniendo detalles de productos de Shopify...")
    products = fetch_shopify_products(
        db_config.SHOPIFY_API_KEY,
        db_config.SHOPIFY_API_PASSWORD,
        db_config.SHOPIFY_STORE_URL,
        SHOPIFY_API_CONFIG['api_version']
    )
    logging.info(f"Total productos obtenidos: {len(products)}")

    if not products:
        print("No hay productos disponibles. Saliendo.")
        exit()

    # 4) Obtener inventory_item_ids
    inventory_item_ids = list({
        variant.get('inventory_item_id') 
        for product in products
        for variant in product.get('variants', [])
        if variant.get('inventory_item_id')
    })
    logging.info(f"Total unique inventory_item_ids: {len(inventory_item_ids)}")

    # 5) Fetch inventory levels para esas ubicaciones
    inventory_levels = fetch_inventory_levels(
        db_config.SHOPIFY_API_KEY,
        db_config.SHOPIFY_API_PASSWORD,
        db_config.SHOPIFY_STORE_URL,
        inventory_item_ids,
        desired_location_ids,
        SHOPIFY_API_CONFIG['api_version']
    )

    # 6) Insert/Update en la DB
    with Database(db_config.DB_HOST, db_config.DB_PORT, db_config.DB_USER, db_config.DB_PASSWORD, db_config.DB_NAME) as conn:
        mappings = insert_or_update_products_variants_and_inventory(products, conn, locations, inventory_levels)
        
        # 7) Comparar e insertar en logs
        if mappings:
            compare_inventories_and_log(conn, inventory_levels, mappings, locations)

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Duración total: {duration:.2f} segundos")
    print(f"Tiempo total: {duration:.2f} segundos\nFIN PROGRAMA")



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


    

        