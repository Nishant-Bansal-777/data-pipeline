
import json
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from utils.stock_market import Record, get_data, get_api_url, Type, logger


default_args = {
    'owner': 'Nishant Bansal',
    'start_date': datetime(2024, 6, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email': 'nishant.bansal777@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

def fetch_data(data_type, **context):
    type_str = data_type.value.lower()
    api_url = get_api_url(data_type)
    records = get_data(api_url)
    logger.info(records)
    context['ti'].xcom_push(key=f'{type_str}_records', value=records)

def stream_data(data_type, **context):
    type_str = data_type.value.lower()
    print(f'fetch_{type_str}_data')
    records = context['ti'].xcom_pull(task_ids=f'fetch_{type_str}_data', key=f'{type_str}_records')
    timeout = 5000
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    except KafkaTimeoutError:
        logger.error(f"TimeoutError: Unable to fetch metadata within {timeout/1000} seconds")
    except Exception as e:
        logger.error(f'error: {e}')

    if producer:
        for item in records:
            record = Record.from_dict(item).to_dict()
            print(record)
            producer.send(topic=type_str, value=json.dumps(record).encode('utf-8'))
            logger.info(record)


with DAG(
    dag_id='stream_gainer_data',
    description='Streams top gainers from stock market',
    default_args=default_args,
    # schedule='@hourly',
    schedule='30 9-15 * * 1-5',
    catchup=False
) as gainer_dag:
    fetch_gainers_task = PythonOperator(
        task_id='fetch_gain_data',
        python_callable=fetch_data,
        op_args=[Type.GAIN],
        provide_context=True
    )
    stream_gainers_task = PythonOperator(
        task_id='stream_gainer',
        python_callable=stream_data,
        op_args=[Type.GAIN],
        provide_context=True
    )


with DAG(
    dag_id='stream_loser_data',
    description='Streams top losers from stock market',
    default_args=default_args,
    # schedule='@hourly',
    schedule='30 9-15 * * 1-5',
    catchup=False
) as loser_dag:
    fetch_losers_task = PythonOperator(
        task_id='fetch_lose_data',
        python_callable=fetch_data,
        op_args=[Type.LOSE],
        provide_context=True
    )
    stream_losers_task = PythonOperator(
        task_id='stream_loser',
        python_callable=stream_data,
        op_args=[Type.LOSE],
        provide_context=True
    )



fetch_gainers_task >> stream_gainers_task
fetch_losers_task >> stream_losers_task
