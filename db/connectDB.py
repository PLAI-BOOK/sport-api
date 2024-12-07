import os
import psycopg2
from dotenv import load_dotenv
# Load environment variables from .env file
# it likes it here dont ask why
load_dotenv()
from db_connect.connectDB import get_db_connection as get_db_connection_out

# i changed it to work with the connecting repo, i hope it works

def get_db_connection(db_name=None):
    return get_db_connection_out(db_name=db_name)
    # """Establish a connection to the database."""
    # DB_HOST = os.getenv("DB_HOST")
    # DB_USER = os.getenv("DB_USER")
    # DB_PASSWORD = os.getenv("DB_PASSWORD")
    # db_name = db_name or os.getenv("DB_NAME").lower()
    # DB_PORT = os.getenv("DB_PORT")
    #
    #
    # try:
    #     conn = psycopg2.connect(
    #         host=DB_HOST,
    #         database=db_name,
    #         user=DB_USER,
    #         password=DB_PASSWORD,
    #         port = DB_PORT
    #     )
    #     return conn
    # except Exception as e:
    #     print(f"Error while connecting to the {db_name} database: {e}")
    #     return None
