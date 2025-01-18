"""
Database connection and query execution handling.
Manages PostgreSQL connections and EXPLAIN ANALYZE execution.
"""
from dataclasses import dataclass
from typing import Dict, Any
import psycopg2
from psycopg2.extensions import connection
from contextlib import contextmanager

@dataclass
class DatabaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config

    @contextmanager
    def get_connection(self) -> connection:
        """Create a database connection using context manager."""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password
            )
            yield conn
        finally:
            if conn:
                conn.close()

    def execute_explain(self, query: str) -> Dict[str, Any]:
        """Run EXPLAIN ANALYZE and return execution plan with row count."""
        explain_query = f"""
        EXPLAIN (
            ANALYZE true,
            BUFFERS true,
            TIMING true,
            COSTS true,
            VERBOSE true,
            FORMAT JSON
        ) {query}"""
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(explain_query)
                plan = cursor.fetchall()[0][0]
                
                cursor.execute(query)
                row_count = len(cursor.fetchall())
                
                return {
                    'plan': plan[0],
                    'row_count': row_count
                }
