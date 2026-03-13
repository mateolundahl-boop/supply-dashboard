import redshift_connector
import pandas as pd
from dotenv import load_dotenv
import sys

def execute_query(query):
    """Ejecuta un query en Redshift y retorna un DataFrame."""
    # Load .env from same directory as this script
    import pathlib
    env_path = pathlib.Path(__file__).parent / '.env'
    load_dotenv(env_path)

    import os
    DB_HOST = "general.cptvidurgnhk.us-west-2.redshift.amazonaws.com"
    DB_NAME = "prod"
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    conn = redshift_connector.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=5439
    )

    cur = conn.cursor()
    conn.autocommit = True

    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

    cur.close()
    conn.close()

    return df

if __name__ == "__main__":
    if len(sys.argv) > 1:
        query = sys.argv[1]
        result = execute_query(query)
        print(result.to_string())
    else:
        print("Uso: python query_runner.py 'SELECT ...'")
