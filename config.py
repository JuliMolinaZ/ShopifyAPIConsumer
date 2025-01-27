#config.py

from dotenv import load_dotenv
import os

class Config:
    def __init__(self):
        load_dotenv(override=True)
        # Shopify Config
        self.SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
        self.SHOPIFY_API_PASSWORD = os.getenv('SHOPIFY_API_PASSWORD')
        self.SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')

        # MySQL Config
        self.DB_HOST = os.getenv('DB_HOST')
        self.DB_PORT = int(os.getenv('DB_PORT', 3306))
        self.DB_USER = os.getenv('DB_USER')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD')
        self.DB_NAME = os.getenv('DB_NAME')

