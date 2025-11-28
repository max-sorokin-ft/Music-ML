import psycopg2
import os

def get_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME"),
            port=os.getenv("DB_PORT", "5432"),
        )
    except Exception as e:
        print(f"Error getting connection: {e}")
        return None