<<<<<<< HEAD
#rate_limiter.py

=======
>>>>>>> cf1991bb448aefee5e9613a2c09467cfd22d4861
import requests
import mysql.connector
import time
import logging
import json
import random
from dotenv import load_dotenv
from collections import deque
from math import ceil
import unicodedata
from config import Config
from db import Database
import os


class RateLimiter:
    def __init__(self, max_calls, period, jitter=True):
        self.calls = deque()
        self.max_calls = max_calls
        self.period = period  # en segundos
        self.jitter = jitter

    def wait(self):
        current = time.time()
        while self.calls and self.calls[0] <= current - self.period:
            self.calls.popleft()
        if len(self.calls) >= self.max_calls:
            wait_time = self.period - (current - self.calls[0])
            if self.jitter:
                wait_time = wait_time / 2 + random.uniform(0, wait_time / 2)
            logging.info(f"Rate limit alcanzado. Esperando {wait_time:.2f} segundos.")
            time.sleep(wait_time)
<<<<<<< HEAD
        self.calls.append(time.time())
=======
        self.calls.append(time.time())
>>>>>>> cf1991bb448aefee5e9613a2c09467cfd22d4861
