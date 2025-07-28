import os
import psycopg2
from typing import Any, Dict, Tuple, List
from backend.extractors.base.base_storage_manager import BaseStorageManager
from psycopg2 import sql
from sqlalchemy import create_engine


class PostgresStorageManager(BaseStorageManager):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.conn_url = config.get("connection_url")
        self.conn = None
        self.engine = create_engine(self.conn_url)

    def connect(self):
        if not self.conn:
            self.conn = psycopg2.connect(self.conn_url)

    def validate_storage(self) -> bool:
        try:
            self.connect()
            return True
        except Exception as e:
            self.logger.error(f"Storage validation failed: {str(e)}")
            return False

    def list_files(self, path: str) -> List[str]:
        # Not applicable to DB storage, return empty or raise NotImplementedError
        return []

    def retrieve_data(self, path: str) -> Any:
        # Optional: retrieve data by table name if needed
        raise NotImplementedError("retrieve_data not implemented for PostgresStorageManager")


    def store_data(self, records, table_name, metadata=None):
        if not records:
            self.logger.warning(f"No records to store in {table_name}")
            return False, table_name

        conn = self.engine.raw_connection()
        cursor = conn.cursor()

        try:
            # Extract columns from first record
            columns = list(records[0].keys())
            column_defs = ", ".join(f'"{col}" TEXT' for col in columns)

            # Create table if not exists
            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {column_defs}
            );
            '''
            cursor.execute(create_table_query)

            # Prepare insert query
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(placeholders)
            )

            for record in records:
                values = [str(record.get(col, '')) for col in columns]
                cursor.execute(insert_query, values)

            conn.commit()
            return True, table_name

        except Exception as e:
            self.logger.error(f"Failed to store data in Postgres: {str(e)}")
            conn.rollback()
            return False, table_name

        finally:
            cursor.close()
            conn.close()
