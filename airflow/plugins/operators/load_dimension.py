from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate_sql = """
        TRUNCATE {} ;
    """
    
    insert_sql = """
        INSERT INTO {} {} 
        {} ;
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 columns="",
                 sql_create_table="",
                 sql_insert_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.sql_create_table = sql_create_table
        self.sql_insert_data = sql_insert_data
        

    def execute(self, context):
#         self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        #create table
        redshift.run(self.sql_create_table)
        logging.info(self.sql_create_table)
        logging.info(f"{self.table} has been created") 
        
        
        # truncate table 
        formatted_truncate_sql = LoadDimensionOperator.truncate_sql.format(self.table)
        redshift.run(formatted_truncate_sql)
        logging.info(formatted_truncate_sql)
        logging.info(f"{self.table} has been truncated") 
        
        
        # insert data 
        formatted_insert_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.columns,
            self.sql_insert_data
        )
        redshift.run(formatted_insert_sql)
        logging.info(formatted_insert_sql)
        logging.info(f"{self.table} has been inserted")
        
        
        
        
        