# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 10:57:58 2020

@author: Nancy~
"""

# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta
import boto3
import configparser

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

s3 = boto3.resource('s3')
config = configparser.ConfigParser()
config.read("s3_properties.ini")
s3_prop = config['s3']
bucket = s3_prop['bucket']
    
def source1_to_s3(filename, key, bucket_name):
    # code that writes our data from source 1 to s3
    s3.Bucket(bucket_name).upload_file(filename)
    return None

    
# def source2_to_hdfs(config, ds, **kwargs):
#     # code that writes our data from source 2 to hdfs
#     # ds: the date of run of the given task.
#     # kwargs: keyword arguments containing context parameters for the run.
#     return None

# def get_hdfs_config():
#     #return HDFS configuration parameters required to store data into HDFS.
#     return None

# config = get_hdfs_config()

dag = DAG(
  dag_id='my_dag', 
  description='processing DAG',
  default_args=default_args)

src1_s3 = PythonOperator(
  task_id='source1_to_s3', 
  python_callable=source1_to_s3,
  op_kwargs={
        'filename': 'DE1_0_*.csv',
        'bucket_name': bucket,
    },
  dag=dag)

# src2_hdfs = PythonOperator(
#   task_id='source2_to_hdfs', 
#   python_callable=source2_to_hdfs, 
#   op_kwargs = {'config' : config},
#   provide_context=True,
#   dag=dag
# )


spark_job = BashOperator(
  task_id='spark_task_etl',
  bash_command='spark-submit --conf spark.driver.maxResultSize=5g \
    --driver-memory 4g --executor-memory 3g \
    --conf spark.shuffle.registration.timeout=50000 \
    --conf spark.sql.shuffle.partitions=1000 \
    --driver-class-path postgresql-42.2.16.jar \
    --jars postgresql-42.2.16.jar \
    --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
    --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
    --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
    --master spark://10.0.0.5:7077 main.py',
  dag = dag)

# setting dependencies
src1_s3 >> spark_job
# src2_hdfs >> spark_job
