import mysql.connector
import logging

class Database:
    def __init__(self, host, port, user, password, database):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': 'utf8mb4',   
            'use_unicode': True 
        }
        self.conn = None

    def __enter__(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            logging.info("Conexión a la base de datos establecida.")
            return self.conn
        except mysql.connector.Error as err:
            logging.error(f"Error al conectar a la base de datos: {err}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            logging.info("Conexión a la base de datos cerrada.")
