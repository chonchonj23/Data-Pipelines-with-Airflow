from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

from airflow.operators.subdag_operator import SubDagOperator
from subdag import Stage_subdag

import sql_statements


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

start_d = datetime(2021, 4, 28)

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# below are hints
# stage_events_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_events',
#     dag=dag
# )

create_staging_events_id = "Stage_events"
create_staging_events_task = SubDagOperator(
    subdag=Stage_subdag(
        parent_dag_name="udac_example_dag",
        task_id=create_staging_events_id,
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        table="staging_events",
        sql_stmt=sql_statements.CREATE_STAGEING_EVENTS_SQL,
        s3_bucket="udacity-dend",
        s3_key="log_data",
        s3_datatype="json",
        s3_method="s3://udacity-dend/log_json_path.json",
        s3_region="us-west-2",
        start_date=start_d,
    ),
    task_id=create_staging_events_id,
    dag=dag,
)


# below are hints
# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag
# )

create_staging_songs_id = "Stage_songs"
create_staging_songs_task = SubDagOperator(
    subdag=Stage_subdag(
        parent_dag_name="udac_example_dag",
        task_id=create_staging_songs_id,
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        table="staging_songs",
        sql_stmt=sql_statements.CREATE_STAGEING_SONGS_SQL,
        s3_bucket="udacity-dend",
        s3_key="song_data",
        s3_datatype="json",
        s3_method="auto",
        s3_region="us-west-2",
        start_date=start_d,
    ),
    task_id=create_staging_songs_id,
    dag=dag,
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    columns="(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)",
    sql_create_table=sql_statements.CREATE_SONGPLAYS_SQL,
    sql_insert_data=SqlQueries.songplay_table_insert
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    columns="(userid, first_name, last_name, gender, level)",
    sql_create_table=sql_statements.CREATE_USER_SQL,
    sql_insert_data=SqlQueries.user_table_insert
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    columns="(songid, title, artistid, year, duration)",
    sql_create_table=sql_statements.CREATE_SONGS_SQL,
    sql_insert_data=SqlQueries.song_table_insert
)

 
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    columns="(artistid, name, location, lattitude, longitude)",
    sql_create_table=sql_statements.CREATE_ARTISTS_SQL,
    sql_insert_data=SqlQueries.artist_table_insert
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    columns="(start_time, hour, day, week, month, year, weekday)",
    sql_create_table=sql_statements.CREATE_TIME_SQL,
    sql_insert_data=SqlQueries.time_table_insert
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [ create_staging_events_task, create_staging_songs_task ] >> load_songplays_table
load_songplays_table >> [ load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table ] >> run_quality_checks
run_quality_checks >> end_operator


# start_operator >> create_staging_events_task >> run_quality_checks


