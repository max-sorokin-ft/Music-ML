import logging
from pathlib import Path

from db.db import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"

def apply_migrations():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename TEXT PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT NOW()
        );
        """
    )
    conn.commit()

    files = sorted(f.name for f in MIGRATIONS_DIR.glob("*.sql"))

    for filename in files:
        cur.execute("SELECT 1 FROM schema_migrations WHERE filename = %s", (filename,))
        if cur.fetchone():
            logger.info("SKIP  %s", filename)
            continue

        logger.info("APPLY %s", filename)
        filepath = MIGRATIONS_DIR / filename

        with open(filepath, "r", encoding="utf-8") as f:
            sql = f.read()
            cur.execute(sql)

        cur.execute(
            "INSERT INTO schema_migrations (filename) VALUES (%s)",
            (filename,),
        )
        conn.commit()

    cur.close()
    conn.close()
    logger.info("Migrations complete.")


if __name__ == "__main__":
    apply_migrations()
