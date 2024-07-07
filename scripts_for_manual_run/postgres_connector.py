from sqlalchemy import create_engine
import logging
import pandas as pd
import psycopg2

import credentials as cr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Postgres_Connector_Logger")


class PostgresConnector:
    """
    Class to represent Postgres connection object with engine.
    Have methods for saving and loading data to/from PostgreSQL.
    """
    ENGINE_URI = cr.AWS_POSTGRES_URI
    engine = create_engine(
        ENGINE_URI,
        pool_pre_ping=True,
    )

    def __init__(self, table_name: str, dtype_dict: dict):

        self.table_name = table_name
        self.dtype_dict = dtype_dict

    def save_df_to_postgres(self, df: pd.DataFrame, if_exists: str = "append") -> bool:
        """Saving data in Postgres table and return True if success, else False"""
        try:
            with self.engine.connect() as connection:
                transaction = connection.begin()
                df.to_sql(self.table_name, connection, if_exists=if_exists, dtype=self.dtype_dict, index=False)
                transaction.commit()
                logger.debug("Transaction committed successfully.")
        except ValueError as e:
            logger.error(f"If chosen mode if_exists='fail' - The table already exists")
            transaction.rollback()
            logger.error("Transaction rolled back due to an error:", e)
            return False
        except psycopg2.OperationalError as e:
            logger.error(f"Connection to server failed")
            transaction.rollback()
            logger.error("Transaction rolled back due to an error:", e)
            return False
        except Exception as e:
            transaction.rollback()
            logger.error("Transaction rolled back due to an error:", e)
            return False
        finally:
            connection.close()
        return True

    @classmethod
    def load_data_from_postgres_to_df_chunks(
            cls,
            chunk_size: int = 100000,
            sql: str = "SELECT chembl_id, canonical_smiles FROM rbukharev.compound_structures WHERE canonical_smiles not NULL ORDER BY chembl_id"
    ) -> list:
        """Loading data from Postgres in chunks with set chunk size and provided sql query"""
        chunks = []
        for chunk in pd.read_sql_query(sql=sql, con=cls.engine, chunksize=chunk_size):
            chunks.append(chunk)

        print(f'All {len(chunks)} chunks loaded')
        return chunks

    @classmethod
    def load_data_from_postgres_to_df(cls, sql: str) -> pd.DataFrame:
        """Loading data from Postgres with provided sql query"""
        res_df = pd.read_sql_query(sql=sql, con=cls.engine)

        return res_df
