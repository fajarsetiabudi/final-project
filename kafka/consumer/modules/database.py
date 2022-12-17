from sqlalchemy import create_engine, text, engine
import logging


def get_engine(host: str, port: int, user: str, password: str):
    db_uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"
    logging.info(f"[DATABASE CONNECT]: url={db_uri}")
    engine = create_engine(db_uri)
    return engine

def insert_data(engine: engine.Engine, data: dict, tablename: str):
    
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {tablename} (currency_code, currency_name, rate, timestamp) 
                VALUES (:currency_code, :currency_name, :rate, :timestamp)
            """),
            [data]
        )