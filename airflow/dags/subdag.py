import datetime

from airflow import DAG
from airflow.models import BaseOperator

from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.udacity_plugin import HasRowsOperator
# from airflow.operators.udacity_plugin import S3ToRedshiftOperator


# from airflow.operators.udacity_plugin import StageToRedshiftOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.hooks.postgres_hook import PostgresHook

from airflow.utils.decorators import apply_defaults

import logging
import sql



def Stage_subdag(
        parent_dag_name,
        task_id,
        aws_credentials_id,
        redshift_conn_id,
        table,
        sql_stmt,
        s3_bucket,
        s3_key,
        s3_datatype,
        s3_method,
        s3_region,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
        
        
        
    create_staging = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=sql_stmt
    )
    
    
    staging_to_redshift = StageToRedshiftOperator(
        task_id=f"load_{table}_from_s3_to_redshift",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        table=table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        s3_region = s3_region,
        s3_datatype = s3_datatype,
        s3_method = s3_method
    )
  
    create_staging >> staging_to_redshift
 
    return dag
        
