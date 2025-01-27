import os
import requests
import time
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SHOPIFY_API_KEY")
PASSWORD = os.getenv("SHOPIFY_API_PASSWORD")
STORE_NAME = os.getenv("SHOPIFY_STORE_URL")

INVENTORY_ITEM_ID = "45118223777837"
API_VERSION = "2023-07"

def fetch_locations(api_key, password, store_name):
    """
    Retorna un dict { location_id: location_name }
    con todas las ubicaciones de tu tienda.
    """
    url = f"https://{store_name}/admin/api/{API_VERSION}/locations.json"
    resp = requests.get(url, auth=(api_key, password))
    if resp.status_code == 200:
        data = resp.json()
        loc_dict = {}
        for loc in data.get("locations", []):
            loc_id = loc["id"]
            loc_name = loc["name"]
            loc_dict[loc_id] = loc_name
        return loc_dict
    else:
        print(f"Error {resp.status_code} al pedir locations: {resp.text}")
        return {}

def fetch_inventory_levels_with_retry(api_key, password, store_name, inventory_item_id, max_attempts=10):
    """
    Llama a /inventory_levels.json?inventory_item_ids=xxx repetidamente si recibe 429,
    usando un backoff incremental para no saturar la API.
    max_attempts define cuántas veces como máximo lo intentará.
    """
    base_url = f"https://{store_name}/admin/api/{API_VERSION}/inventory_levels.json"
    params = {
        "inventory_item_ids": inventory_item_id
    }

    attempt = 0
    wait_time = 2  # Espera inicial en segundos

    while attempt < max_attempts:
        attempt += 1
        print(f"Intento #{attempt} solicitando inventory_levels para item_id={inventory_item_id}... (espera={wait_time}s)")

        resp = requests.get(base_url, auth=(api_key, password), params=params)
        if resp.status_code == 200:
            return resp.json()  # Éxito: retornamos el JSON completo
        elif resp.status_code == 429:
            print(f"Respuesta 429: {resp.text}")
            # Dormimos un poco y aumentamos el backoff
            time.sleep(wait_time)
            wait_time *= 2
        else:
            # Otro error distinto de 429
            print(f"Error {resp.status_code} al pedir inventory_levels: {resp.text}")
            return None

    # Si llegamos aquí, es que superamos max_attempts con 429
    print("Se alcanzó el número máximo de reintentos sin éxito.")
    return None

def main():
    if not API_KEY or not PASSWORD or not STORE_NAME:
        print("Faltan credenciales en .env")
        return
    
    # 1. Obtener nombres de ubicación en un dict { location_id: location_name }
    loc_dict = fetch_locations(API_KEY, PASSWORD, STORE_NAME)
    if not loc_dict:
        print("No se pudo obtener la lista de ubicaciones, o está vacía.")
    
    # 2. Obtener niveles de inventario para INVENTORY_ITEM_ID
    data = fetch_inventory_levels_with_retry(API_KEY, PASSWORD, STORE_NAME, INVENTORY_ITEM_ID, max_attempts=10)
    if data is None:
        print("No fue posible obtener inventory_levels (demasiados 429 o error).")
        return
    
    levels = data.get("inventory_levels", [])
    if not levels:
        print(f"No se encontraron ubicaciones con stock para inventory_item_id={INVENTORY_ITEM_ID}.")
        return
    
    # 3. Mostrar location_id, location_name, stock
    print(f"\n=== Resultados de inventory_levels para item {INVENTORY_ITEM_ID} ===\n")
    for lvl in levels:
        loc_id = lvl["location_id"]
        available = lvl.get("available", 0)
        # Tomar el nombre de la ubicación, o "Unknown" si no está
        loc_name = loc_dict.get(loc_id, "Unknown")
        print(f" - location_id={loc_id} ({loc_name}), stock={available}")

if __name__ == "__main__":
    main()

