import os

from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
PRODUCTS_SHEET_NAME = os.getenv("PRODUCTS_SHEET_NAME", "products")
ORDERS_SHEET_NAME = os.getenv("ORDERS_SHEET_NAME", "orders")
CUSTOMERS_SHEET_NAME = os.getenv("CUSTOMERS_SHEET_NAME", "customers_b2b")
CONFIG_BOT_SHEET_NAME = os.getenv("CONFIG_BOT_SHEET_NAME", "config_bot")
CONFIG_SITE_SHEET_NAME = os.getenv("CONFIG_SITE_SHEET_NAME", "config_site")
