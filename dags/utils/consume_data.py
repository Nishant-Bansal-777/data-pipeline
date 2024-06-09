from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import logging

logging.basicConfig(level=logging.INFO)

KEYSPACE = 'spark_streams'
TABLE_NAME = 'stock_market_data'

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """ % (KEYSPACE,))
    logging.info("Keyspace created successfully!")

def drop_table(session):
    try:
        session.execute(f"DROP TABLE IF EXISTS {KEYSPACE}.{TABLE_NAME};")
        logging.info(f"Table '{TABLE_NAME}' dropped successfully from keyspace '{KEYSPACE}'.")
    except Exception as e:
        logging.error(f"Error dropping table '{TABLE_NAME}' from keyspace '{KEYSPACE}': {e}")

def create_table(session):
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE_NAME} (
        fincode INT,
        s_name TEXT,
        close_price FLOAT,
        prevclose FLOAT,
        perchg FLOAT,
        netchg FLOAT,
        volume FLOAT,
        timestamp TIMESTAMP,
        topic TEXT,
        PRIMARY KEY (fincode, timestamp)
    );
    """)
    logging.info("Table created successfully!")


def print_tables(session):
    query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{KEYSPACE}';"
    rows = session.execute(query)
    for row in rows:
        print(row.table_name)


if __name__ == '__main__':
    """local testing"""
    cluster = Cluster(['localhost'])
    session = cluster.connect()

    create_keyspace(session)

    session.set_keyspace('spark_streams')

    create_table(session)

    bootstrap_servers = ['localhost:9021']
    topics = ['lose', 'gain']


    consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=['localhost:9092'],
            group_id='stock_data',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x)

    for message in consumer:
        print(11111111111111111, type(message), message)
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

    print_tables(session)