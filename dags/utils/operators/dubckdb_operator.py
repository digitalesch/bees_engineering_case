from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import duckdb
from deltalake import DeltaTable, write_deltalake

class DuckDBOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, 
        sql: str, 
        output_path: str = None,
        duckdb_conn: str = "my_database.duckdb", 
        parition_by: str = [],
        **kwargs
    ):
        """
        :param sql: The SQL query to execute
        :param duckdb_conn: Path to the DuckDB database file
        :param kwargs: Additional parameters passed to the BaseOperator
        """
        super().__init__(**kwargs)
        self.sql = sql
        self.duckdb_conn = duckdb_conn
        self.output_path = output_path
        self.parition_by = parition_by

    def execute(self, context):
        self.log.info(f"Executing DuckDB query: {self.sql}")
        
        # Connect to the DuckDB database
        connection = duckdb.connect(database=self.duckdb_conn)
        
        try:
            # Execute the query
            result = connection.execute(self.sql).df()
            self.log.info(f"Query result: {result}")
            if self.output_path:
                self.log.info(f"Saving to delta: {self.output_path}")
                write_deltalake(self.output_path,result,mode='overwrite',partition_by=self.parition_by)
            # return result
        except duckdb.IOException:
            self.log.info(f"No files found")
        except Exception as e:
            self.log.error(f"Error executing query: {e}")
            raise e
        
        finally:
            # Close the connection
            connection.close()
