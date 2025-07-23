import psycopg2
import os
from base.base_storage_manager import BaseStorageManager

class PostgresStorageManager(BaseStorageManager):
    def __init__(self):
        self.connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "postgres"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "mysecretpassword"),
            port=os.getenv("POSTGRES_PORT", 5432)
        )
        self.cursor = self.connection.cursor()

    def connect(self):
        pass

    def validate_storage(self):
        return self.connection.closed == 0

    def store_data(self, data, destination):
        self.save(data, destination)

    def retrieve_data(self, identifier):
        raise NotImplementedError

    def list_files(self):
        raise NotImplementedError

    def save(self, data, table_name):
        if not data:
            print("No data to save.")
            return

        columns = data[0].keys()
        column_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        create_stmt = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            {", ".join([f'"{col}" TEXT' for col in columns])}
        );
        """
        self.cursor.execute(create_stmt)

        for row in data:
            values = [str(row.get(col, "")) for col in columns]
            quoted_columns = ", ".join([f'"{col}"' for col in columns])
            insert_stmt = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders});'
            self.cursor.execute(insert_stmt, values)

        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()
