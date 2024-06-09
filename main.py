'''
#airflow
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up

#kafka https://jskim1991.medium.com/docker-docker-compose-example-for-kafka-zookeeper-and-schema-registry-c516422532e7
pip install -r requirements.txt

docker exec -it 1fcd0ff4e579 /bin/bash
airflow tasks test stream_market_data fetch_data_task
airflow dags test stream_market_data


docker exec -it <container_id_or_name> airflow dags trigger <dag_id>
docker exec -it fdca57b15a52 airflow dags trigger stream_gainer_data
docker exec -it fdca57b15a52 airflow tasks logs stream_gainer_data fetch_gain_data

docker exec -it 74b844568d02 cassandra cqlsh -u cassandra -p cassandra localhost 9042


cqlsh:
USE spark_streams;
DESCRIBE TABLES;
DESCRIBE TABLES IN spark_streams;


/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --class org.apache.spark.examples.SparkPi /path/to/spark-examples_2.13-3.4.1.jar 100

'''
