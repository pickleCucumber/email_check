import os
from dotenv import load_dotenv
import redis

load_dotenv()
# ---------- DB ----------
host = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
log = os.getenv("DB_USER")
psw = os.getenv("DB_PASSWORD")

DRIVER_NAME = "ODBC Driver 17 for SQL Server"

DB_URL = (
    f"mssql+pyodbc://{log}:{psw}@{host}/{DB_NAME}?driver={DRIVER_NAME}"
)

# ---------- Redis ----------
REDIS_HOST = os.getenv("REDIS_HOST_TEST")
REDIS_PORT = os.getenv("REDIS_PORT_TEST")
REDIS_DB = int(os.getenv("REDIS_DB_TEST", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASS")
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)#, decode_responses=True)

# alternative
# REDIS_HOST = os.getenv("REDIS_HOST_TEST")
# REDIS_PORT = os.getenv("REDIS_PORT_TEST")
# REDIS_DB = os.getenv("REDIS_DB_TEST")
# REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
# REDIS_SSL = os.getenv("REDIS_SSL", "false").lower() == "true"
#
# redis_kwargs = {
#     "host": REDIS_HOST,
#     "port": REDIS_PORT,
#     "db": REDIS_DB,
#     "password": REDIS_PASSWORD,
#     "decode_responses": True,
# }
# if REDIS_SSL:
#     redis_kwargs["ssl"] = True
#     redis_kwargs["ssl_cert_reqs"] = None