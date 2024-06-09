import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

"""
https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
https://spark.apache.org/docs/2.1.1/structured-streaming-kafka-integration.html
"""
def create_keyspace(session):
    print(11111111111111111111111111)
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    print(2222222222222222222222)
    session.execute("""
    CREATE TABLE IF NOT EXISTS stock_market_data (
        fincode INT,
        s_name TEXT,
        close_price FLOAT,
        prevclose FLOAT,
        perchg FLOAT,
        netchg FLOAT,
        volume FLOAT,
        timestamp TIMESTAMP,
        PRIMARY KEY (fincode)
    );
    """)

    print("Table created successfully!")


def insert_data(session, data):
    print("Inserting data...")

    fincode = data.get('FINCODE')
    s_name = data.get('S_NAME')
    close_price = data.get('CLOSE_PRICE')
    prevclose = data.get('PREVCLOSE')
    perchg = data.get('PERCHG')
    netchg = data.get('NETCHG')
    volume = data.get('VOLUME')
    timestamp = data.get('TIMESTAMP')

    try:
        session.execute("""
            INSERT INTO spark_streams.stock_market_data (fincode, s_name, close_price, prevclose, perchg, netchg, volume, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (fincode, s_name, close_price, prevclose, perchg, netchg, volume, timestamp))
        logging.info(f"Data inserted for FINCODE: {fincode}")

    except Exception as e:
        logging.error(f'Could not insert data due to {e}')


def create_spark_connection():
    print(3333333333333333333333)
    s_conn = None

    try:

        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        print(00000000000000000000000000000000000)
        s_conn.sparkContext.setLogLevel("INFO")
        print(11111111)
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    print(4444444444444444444444)
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'lose,gain') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    print(555555555555555555555555)
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    print(666666666666666666666)
    schema = StructType([
        StructField("FINCODE", StringType(), False),
        StructField("S_NAME", StringType(), False),
        StructField("CLOSE_PRICE", StringType(), False),
        StructField("PREVCLOSE", StringType(), False),
        StructField("PERCHG", StringType(), False),
        StructField("NETCHG", StringType(), False),
        StructField("VOLUME", StringType(), False),
        StructField("TIMESTAMP", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel



if __name__ == "__main__":

    spark_conn = create_spark_connection()

    # if spark_conn is not None:
    #
    #     spark_df = connect_to_kafka(spark_conn)
    #     selection_df = create_selection_df_from_kafka(spark_df)
    #     session = create_cassandra_connection()
    #
    #     if session is not None:
    #         create_keyspace(session)
    #         create_table(session)
    #
    #         logging.info("Streaming is being started...")
    #
    #         streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
    #                            .option('checkpointLocation', '/tmp/checkpoint')
    #                            .option('keyspace', 'spark_streams')
    #                            .option('table', 'stock_market_data')
    #                            .start())
    #
    #         streaming_query.awaitTermination()