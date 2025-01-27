import os
import requests
import mysql.connector
from datetime import datetime, timedelta
import time
import base64
import logging
import sys
from variants.translation import translate_status
from dotenv import load_dotenv
from zoneinfo import ZoneInfo 

# Cargar variables de entorno
load_dotenv()

# Configuración del logging
logging.basicConfig(
    filename='orderslastsync.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

# Agregar un logger a la consola para ver los logs en tiempo real
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# Credenciales de la API de Shopify desde variables de entorno
api_key = os.getenv("SHOPIFY_API_KEY")
api_password = os.getenv("SHOPIFY_API_PASSWORD")
store_url = os.getenv("SHOPIFY_STORE_URL")  # URL de la tienda Shopify

# Conexión a la base de datos MySQL usando variables de entorno
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# Configurar la zona horaria de Ciudad de México
mexico_tz = ZoneInfo("America/Mexico_City")

# Función para determinar el estado de la orden en Shopify
def determine_order_status(order):
    if order.get('cancelled_at'):
        return 'Canceled'
    elif order.get('closed_at'):
        return 'Archived'
    else:
        return 'Open'

# Función para obtener el estado de devolución de una orden
def fetch_return_status(api_key, password, store_name, order_id):
    base_url = f"https://{store_name}.myshopify.com/admin/api/2023-07/orders/{order_id}/refunds.json"
    
    credentials = f"{api_key}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {encoded_credentials}'
    }

    response = requests.get(base_url, headers=headers)
    if response.status_code == 200:
        refunds = response.json().get('refunds', [])
        if refunds:
            logging.info(f"Return status fetched for order_id {order_id}: open")
            return 'open' if any('status' in refund and refund['status'] == 'open' for refund in refunds) else 'closed'
        else:
            logging.info(f"No return status for order_id {order_id}: none")
            return 'none'
    logging.warning(f"Error fetching return status for order_id {order_id}: {response.status_code} {response.text}")
    return 'none'

# Función para obtener las últimas 400 órdenes de Shopify con paginación
def fetch_shopify_orders(api_key, password, store_name):
    base_url = f"https://{store_name}.myshopify.com/admin/api/2023-07/orders.json"
    limit = 250
    orders = []
    page_info = None

    credentials = f"{api_key}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {encoded_credentials}'
    }

    while len(orders) < 400:
        if page_info:
            url = f"{base_url}?limit={limit}&page_info={page_info}"
        else:
            url = f"{base_url}?limit={limit}&order=created_at desc&status=any"
        
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            fetched_orders = data.get('orders', [])
            orders.extend(fetched_orders)
            logging.info(f"Fetched {len(fetched_orders)} orders from Shopify.")

            if len(fetched_orders) < limit:
                break
            else:
                link_header = response.headers.get('Link')
                if link_header and 'rel="next"' in link_header:
                    next_link = [link for link in link_header.split(',') if 'rel="next"' in link]
                    if next_link:
                        page_info = next_link[0].split(';')[0].split('page_info=')[-1].strip('<> ')
                        logging.info(f"Page info retrieved for next page: {page_info}")
                    else:
                        break
                else:
                    break
        elif response.status_code == 429:
            retry_after_value = response.headers.get('Retry-After', '5')
            try:
                retry_after = float(retry_after_value)
            except ValueError:
                logging.warning(f"Invalid Retry-After value '{retry_after_value}'. Using default of 5 seconds.")
                retry_after = 5.0
            logging.warning(f"Rate limit reached. Retrying after {retry_after} seconds.")
            time.sleep(retry_after)
        elif response.status_code == 401:
            logging.error(f"Authentication error: {response.status_code} {response.text}")
            break
        else:
            logging.error(f"Unexpected error fetching orders: {response.status_code} {response.text}")
            break

    logging.info(f"Total orders fetched: {len(orders)}")
    return orders[:400]

# Función para obtener los detalles de una variante
def fetch_variant_details(api_key, password, store_name, variant_id):
    base_url = f"https://{store_name}.myshopify.com/admin/api/2023-07/variants/{variant_id}.json"
    
    credentials = f"{api_key}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {encoded_credentials}'
    }

    response = requests.get(base_url, headers=headers)
    if response.status_code == 200:
        variant = response.json().get('variant', {})
        return variant
    else:
        logging.warning(f"Error fetching variant details for variant_id {variant_id}: {response.status_code} {response.text}")
    return None

# Función para insertar o actualizar pedidos y elementos de pedidos en MySQL
def insert_or_update_orders_and_items(orders):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    max_retries = 3

    variant_cache = {}
    processed_orders = 0
    updated_orders = 0
    inserted_orders = 0
    skipped_items = 0
    updated_items = 0
    inserted_items = 0

    for order in orders:
        order_id = order['id']
        order_number = order['order_number']
        shipping_address = order.get('shipping_address', {})
        customer_info = order.get('customer', {})
        note = order.get('note', '')
        location_name = order.get('location_name', 'Unknown')

        customer_name = 'Unknown'
        if shipping_address:
            customer_name = shipping_address.get('name', 'Unknown')
        elif customer_info:
            first_name = customer_info.get('first_name', '')
            last_name = customer_info.get('last_name', '')
            customer_name = f"{first_name} {last_name}".strip() or 'Unknown'

        total_price = order.get('total_price', '0.00')
        created_at_str = order.get('created_at')
        created_at = datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%S%z').astimezone(mexico_tz).replace(tzinfo=None) if created_at_str else None
        order_status = translate_status('financial_status', order.get('financial_status', 'Unknown'))
        fulfillment_status = translate_status('fulfillment_status', order.get('fulfillment_status', 'no completado'))
        return_status = translate_status('return_status', fetch_return_status(api_key, api_password, store_url.split('.')[0], order_id))

        StatusShopify = determine_order_status(order)
        tags = order.get('tags', '')

        retries = 0
        while retries < max_retries:
            try:
                cursor.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
                existing_order = cursor.fetchone()

                if existing_order:
                    if (existing_order.get('Customer_name') != customer_name or 
                        existing_order.get('total_price') != total_price or 
                        existing_order.get('created_at') != created_at or
                        existing_order.get('order_status') != order_status or
                        existing_order.get('fulfillment_status') != fulfillment_status or
                        existing_order.get('order_number') != order_number or
                        existing_order.get('return_status') != return_status or
                        existing_order.get('note') != note or
                        existing_order.get('location_name') != location_name or
                        existing_order.get('StatusShopify') != StatusShopify or
                        existing_order.get('tags') != tags):
                        sql = """
                        UPDATE orders 
                        SET Customer_name = %s, total_price = %s, created_at = %s, order_status = %s, 
                            fulfillment_status = %s, order_number = %s, return_status = %s, note = %s, 
                            location_name = %s, StatusShopify = %s, tags = %s
                        WHERE id = %s
                        """
                        values = (customer_name, total_price, created_at, order_status, fulfillment_status,
                                  order_number, return_status, note, location_name, StatusShopify, tags, order_id)
                        cursor.execute(sql, values)
                        logging.info(f"Order {order_id} updated successfully.")
                        updated_orders += 1
                else:
                    sql = """
                    INSERT INTO orders (id, Customer_name, total_price, created_at, order_status, 
                                        fulfillment_status, order_number, return_status, note, location_name, 
                                        StatusShopify, tags)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (order_id, customer_name, total_price, created_at, order_status, 
                              fulfillment_status, order_number, return_status, note, location_name, 
                              StatusShopify, tags)
                    cursor.execute(sql, values)
                    logging.info(f"Order {order_id} inserted successfully.")
                    inserted_orders += 1

                # Procesar los elementos de la orden (order items)
                line_items = order.get('line_items', [])
                for item in line_items:
                    item_id = item['id']
                    product_id = item.get('product_id')
                    variant_id = item.get('variant_id')
                    title = item.get('title', '')
                    quantity = item.get('quantity', 0)
                    price = item.get('price', '0.00')
                    sku = item.get('sku', '')
                    barcode = ''

                    # Obtener el barcode de la variante
                    if variant_id:
                        if variant_id in variant_cache:
                            variant = variant_cache[variant_id]
                        else:
                            variant = fetch_variant_details(api_key, api_password, store_url.split('.')[0], variant_id)
                            if variant:
                                variant_cache[variant_id] = variant
                        barcode = variant.get('barcode', '') if variant else ''

                    if not barcode:
                        logging.error(f"Order item {item_id} for order {order_id} does not have a barcode. Skipping item.")
                        with open('missing_barcodes.log', 'a') as f:
                            f.write(f"Order ID: {order_id}, Item ID: {item_id}, Product ID: {product_id}, Title: {title}\n")
                        skipped_items += 1
                        continue

                    cursor.execute("SELECT * FROM order_items WHERE id = %s", (item_id,))
                    existing_item = cursor.fetchone()

                    if existing_item:
                        if (existing_item.get('product_id') != product_id or
                            existing_item.get('title') != title or
                            existing_item.get('quantity') != quantity or
                            existing_item.get('price') != price or
                            existing_item.get('sku') != sku or
                            existing_item.get('barcode') != barcode or
                            existing_item.get('order_id') != order_id):
                            sql = """
                            UPDATE order_items 
                            SET product_id = %s, variant_id = %s, title = %s, quantity = %s, price = %s, 
                                sku = %s, barcode = %s, order_id = %s
                            WHERE id = %s
                            """
                            values = (product_id, variant_id, title, quantity, price, sku, barcode, order_id, item_id)
                            cursor.execute(sql, values)
                            logging.info(f"Order item {item_id} updated successfully.")
                            updated_items += 1
                    else:
                        sql = """
                        INSERT INTO order_items (id, order_id, product_id, variant_id, title, quantity, price, sku, barcode, ValidacionSku, image_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        values = (item_id, order_id, product_id, variant_id, title, quantity, price, sku, barcode, '', '')
                        cursor.execute(sql, values)
                        logging.info(f"Order item {item_id} inserted successfully.")
                        inserted_items += 1

                # Insertar en LogsSincronizacion
                try:
                    # Obtener hora_creacion_shopify en Ciudad de México
                    hora_creacion_shopify = datetime.strptime(order.get('created_at'), '%Y-%m-%dT%H:%M:%S%z').astimezone(mexico_tz).replace(tzinfo=None)

                    # Obtener hora_creacion_base del campo created_at ya convertido
                    hora_creacion_base = created_at

                    # Obtener ultima_actualizacion como el momento actual en Ciudad de México
                    ultima_actualizacion = datetime.now(mexico_tz).replace(tzinfo=None)

                    insert_log_sql = """
                    INSERT INTO LogsSincronizacion (
                        order_id,
                        hora_creacion_shopify,
                        hora_creacion_base,
                        fulfillment_status,
                        return_status,
                        status_shopify,
                        ultima_actualizacion
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    log_values = (
                        order_id,
                        hora_creacion_shopify,
                        hora_creacion_base,
                        fulfillment_status,
                        return_status,
                        StatusShopify,
                        ultima_actualizacion
                    )
                    cursor.execute(insert_log_sql, log_values)
                    logging.info(f"Log inserted for order {order_id} successfully.")
                except mysql.connector.Error as log_err:
                    logging.error(f"Error inserting log for order {order_id}: {log_err}")
                except Exception as log_e:
                    logging.error(f"Unexpected error inserting log for order {order_id}: {log_e}")

                conn.commit()
                break
            except mysql.connector.Error as err:
                if err.errno == 1205:
                    retries += 1
                    logging.warning(f"Lock wait timeout exceeded for order {order_id}. Retry {retries} of {max_retries}")
                    time.sleep(2)
                else:
                    logging.error(f"Unexpected MySQL error for order {order_id}: {err}")
                    break
            except Exception as e:
                logging.error(f"An unexpected error occurred for order {order_id}: {e}")
                break

    cursor.close()
    conn.close()
    logging.info("Summary of operations:")
    logging.info(f"Processed orders: {len(orders)}")
    logging.info(f"Inserted orders: {inserted_orders}")
    logging.info(f"Updated orders: {updated_orders}")
    logging.info(f"Skipped items due to missing barcode: {skipped_items}")
    logging.info(f"Inserted items: {inserted_items}")
    logging.info(f"Updated items: {updated_items}")

# Bucle infinito para ejecutar el script cada 3 minutos
while True:
    try:
        orders = fetch_shopify_orders(api_key, api_password, store_url.split('.')[0])
        logging.info(f"Total orders fetched: {len(orders)}")

        insert_or_update_orders_and_items(orders)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE sync_info SET last_sync = NOW() ORDER BY id DESC LIMIT 1")
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Last sync updated and database connection closed.")

        time.sleep(180)  # Esperar 3 minutos
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        time.sleep(180)  # Esperar 3 minutos antes de reintentar