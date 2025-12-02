import psycopg2
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


def get_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("PGHOST"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            dbname=os.getenv("PGDATABASE"),
            port=os.getenv("PGPORT"),
        )
    except Exception as e:
        logger.error(f"Error getting connection: {e}")
        raise
