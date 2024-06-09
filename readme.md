# Realtime Data ETL

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)
- [Getting Started](#getting-started)
- [Watch the Video Tutorial](#watch-the-video-tutorial)

## Introduction

This project aims to provide a detailed roadmap for constructing a data engineering pipeline specifically designed for stock market data. I have covered every aspect, from data ingestion to processing and storage, utilizing cutting-edge technologies and best practices in the field.
## System Architecture
![System Architecture](https://github.com/Nishant-Bansal-777/blob/main/architecture.jpeg)

The project is designed with the following components:

- **Data Source**: I used an API to fetch share market (Top, Losers) data for this pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Cassandra**: Where the processed data will be stored.

## Steps

- Setting up a data pipeline with Apache Airflow
- Real-time data streaming with Apache Kafka
- Distributed synchronization with Apache Zookeeper
- Data storage solutions with Cassandra and PostgreSQL
- Containerizing entire setup with Docker

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Cassandra
- PostgreSQL
- Docker

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/Nishant-Bansal-777/data-pipeline.git
    ```

2. Navigate to the project directory:
    ```bash
    cd poc
    ```

3. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up -d
    ```
4. Endpoints    
   ```bash
    airflow -> http://localhost:8080/
    control center -> http://localhost:9021/
    ```

For more detailed instructions, please check out the video tutorial linked below.

## Watch the Video Tutorial

For a complete walkthrough and practical demonstration, check out [Loom Video Tutorial](https://www.youtube.com/watch?v=GqAcTrqKcrY).