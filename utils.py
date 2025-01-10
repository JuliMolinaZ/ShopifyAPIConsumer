import unicodedata
import logging

def clean_string(s):
    """
    Limpia una cadena eliminando espacios y escapando caracteres problemáticos.
    """
    if s:
        return s.strip().replace("'", "''")
    return 'Unknown'

def validate_barcode(barcode):
    """
    Valida si un código de barras es válido según las reglas definidas.
    """
    return bool(barcode and barcode.strip())

# Función para normalizar strings (eliminar acentos y convertir a mayúsculas)
def normalize_string(s):
    """
    Normaliza una cadena eliminando acentos y convirtiendo a mayúsculas.

    Args:
        s (str): Cadena de entrada.

    Returns:
        str: Cadena normalizada.
    """
    if not s:
        return ''
    try:
        nfkd_form = unicodedata.normalize('NFKD', s)
        only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('utf-8')
        return only_ascii.upper()
    except Exception as e:
        logging.error(f"Error normalizando la cadena '{s}': {e}")
        return ''