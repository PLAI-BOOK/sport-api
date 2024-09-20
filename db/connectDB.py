import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_db_connection(db_name=None):
    """Establish a connection to the database."""
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    db_name = db_name or os.getenv("DB_NAME").lower()

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=db_name,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error while connecting to the {db_name} database: {e}")
        return None
