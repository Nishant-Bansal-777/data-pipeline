
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
from utils.consume_data import create_keyspace, create_table
import logging
import time
logging.basicConfig(level=logging.INFO)


cluster = Cluster(['cassandra'])
session = cluster.connect()


default_args = {
    'owner': 'Nishant Bansal',
    'start_date': datetime(2024, 6, 9),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email': 'nishant.bansal777@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

def consume_kafka_messages(session):
    bootstrap_servers = ['broker:29092']
    topics = ['lose', 'gain']
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id='stock_data',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x)
    count = 0
    # duration_minutes = 1
    start_time = time.time()
    # while True:
    #     elapsed_time = time.time() - start_time
    #     print(elapsed_time)
    #     if elapsed_time >= duration_minutes * 0.5:
    #         print(f'Stopping after {elapsed_time}!!!')
    #         break
    #
    #     message = next(consumer)
    for message in consumer:
        try:
            data = json.loads(message.value.decode('utf-8'))
            logging.info(f"Received message: {data}")
            topic = message.topic
            insert_query = """
                INSERT INTO spark_streams.stock_market_data (fincode, s_name, close_price, prevclose, perchg, netchg, volume, timestamp, topic)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            session.execute(insert_query, (
                data['FINCODE'], data['S_NAME'], data['CLOSE_PRICE'], data['PREVCLOSE'],
                data['PERCHG'], data['NETCHG'], data['VOLUME'], data['TIMESTAMP'], topic
            ))
            logging.info("Data inserted into Cassandra successfully")
        except Exception as e:
            logging.error(f"Error processing message: {e}")
        count += 1
        # if count == 10:
        #     break


dag = DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    description='to consume messages from Kafka and insert them into Cassandra',
    schedule=None,
    catchup=False
)

create_keyspace_task = PythonOperator(
    task_id='create_keyspace',
    python_callable=create_keyspace,
    op_kwargs={'session': session},
    dag=dag,
)


create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    op_kwargs={'session': session},
    dag=dag,
)

consume_kafka_messages_task = PythonOperator(
    task_id='consume_kafka_messages',
    python_callable=consume_kafka_messages,
    op_kwargs={'session': session},
    dag=dag,
)



create_keyspace_task >> create_table_task >> consume_kafka_messages_task