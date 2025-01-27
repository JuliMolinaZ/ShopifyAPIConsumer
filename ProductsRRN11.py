import requests
import mysql.connector
import base64
import time
from dotenv import load_dotenv
import os
import re

load_dotenv()

api_key = os.getenv("SHOPIFY_API_KEY")
api_password = os.getenv("SHOPIFY_API_PASSWORD")
store_url = os.getenv("SHOPIFY_STORE_URL") 

def create_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=os.getenv("DB_PORT")
    )

def clean_string(value):
    if value is None:
        return 'Unknown'
    return value.encode('ascii', 'ignore').decode('ascii')

def fetch_shopify_products(api_key, password, store_name):
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

    while url:
        response = session.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            fetched_products = data.get('products', [])
            products.extend(fetched_products)

            link_header = response.headers.get('Link')
            if link_header:
                links = link_header.split(',')
                url = None
                for link in links:
                    if 'rel="next"' in link:
                        url = link[link.find('<') + 1:link.find('>')]
                        break
            else:
                url = None
        elif response.status_code == 429:
            retry_after = int(float(response.headers.get('Retry-After', 5)))
            time.sleep(retry_after)
        elif response.status_code == 401:
            print(f"Error de autenticación: {response.status_code} {response.text}")
            print("Verifica tu API key y password, o asegúrate de que no han expirado.")
            break
        else:
            print(f"Unexpected error: {response.status_code} {response.text}")
            break

    session.close()
    return products

def insert_or_update_products_variants_and_inventory(products, conn):
    cursor = conn.cursor()
    product_values = []
    variant_values = []
    log_values = []

    for product in products:
        if product.get('status') in ['archived', 'draft']:
            continue

        product_id = product['id']
        title = clean_string(product.get('title', 'Unknown'))
        vendor = clean_string(product.get('vendor', 'Unknown'))
        price = product.get('variants', [{}])[0].get('price', '0.00')
        sku = clean_string(product.get('variants', [{}])[0].get('sku', 'Unknown'))
        image_url = clean_string(product.get('image', {}).get('src', 'Unknown')) if product.get('image') else 'Unknown'

        product_values.append((product_id, title, vendor, price, sku, image_url))

        for variant in product.get('variants', []):
            variant_id = variant.get('id')
            variant_title = clean_string(variant.get('title', 'Unknown'))
            variant_sku = clean_string(variant.get('sku', 'Unknown'))
            variant_price = variant.get('price', '0.00')
            variant_stock = variant.get('inventory_quantity', 0)
            variant_barcode = clean_string(variant.get('barcode', 'Unknown'))

            # Obtener el stock anterior para registrar el log
            cursor.execute("SELECT stock FROM product_variants WHERE variant_id = %s", (variant_id,))
            result = cursor.fetchone()
            previous_stock = result[0] if result else None

            if previous_stock is not None and previous_stock != variant_stock:
                diferencia = variant_stock - previous_stock
                log_values.append((
                    variant.get('inventory_item_id'),
                    variant.get('location_id'),
                    previous_stock,
                    variant_stock,
                    diferencia,
                    variant_barcode,
                    product_id,
                    variant_id,
                    'Unknown'  # nombre_ubicacion, podría actualizarse si se tiene esa información
                ))

            variant_values.append((variant_id, product_id, variant_title, variant_sku, variant_price, variant_stock, variant_barcode))

    sql_product = """
    INSERT INTO productos (product_id, title, vendor, price, sku, image_url)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE title=VALUES(title), vendor=VALUES(vendor), price=VALUES(price), sku=VALUES(sku), image_url=VALUES(image_url)
    """
    cursor.executemany(sql_product, product_values)

    sql_variant = """
    INSERT INTO product_variants (variant_id, product_id, title, sku, price, stock, barcode)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE title=VALUES(title), sku=VALUES(sku), price=VALUES(price), stock=VALUES(stock), barcode=VALUES(barcode)
    """
    cursor.executemany(sql_variant, variant_values)

    if log_values:
        sql_log = """
        INSERT INTO LogsInventario (inventory_item_id, location_id, cantidad_anterior, cantidad_nueva, diferencia, barcode, producto_id, variante_id, nombre_ubicacion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(sql_log, log_values)

    conn.commit()
    cursor.close()

def delete_archived_or_draft_products(products, conn):
    cursor = conn.cursor()
    archived_or_draft_products = []

    for product in products:
        if product.get('status') in ['archived', 'draft']:
            product_id = product['id']
            archived_or_draft_products.append(product_id)

            # Verificar si el producto está en la base de datos
            cursor.execute("SELECT * FROM productos WHERE product_id = %s", (product_id,))
            result = cursor.fetchone()
            if result:
                print(f"Producto con ID {product_id} está archivado o en borrador en Shopify y está presente en la base de datos. Eliminando...")
                # Primero eliminar las variantes asociadas al producto
                cursor.execute("DELETE FROM product_variants WHERE product_id = %s", (product_id,))
                # Luego eliminar el producto
                cursor.execute("DELETE FROM productos WHERE product_id = %s", (product_id,))
                conn.commit()
            else:
                print(f"Producto con ID {product_id} está archivado o en borrador en Shopify pero NO está en la base de datos.")

    cursor.close()
    print(f"Total de productos archivados o en borrador encontrados y eliminados: {len(archived_or_draft_products)}")

while True:
    try:
        conn = create_db_connection()
        
        products = fetch_shopify_products(api_key, api_password, store_url.split('.')[0])
        if products:
            insert_or_update_products_variants_and_inventory(products, conn)
            delete_archived_or_draft_products(products, conn)
            print(f"Total products fetched and processed: {len(products)}")
        else:
            print("No products fetched from Shopify.")

        conn.close()
    except Exception as e:
        print(f"Error occurred: {e}")

    time.sleep(180)
