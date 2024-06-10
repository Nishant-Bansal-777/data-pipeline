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
![System Architecture](https://github.com/Nishant-Bansal-777/data-pipeline/blob/staging/architecture.jpeg)

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
    cd data-pipeline
    ```

3. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up -d
    ```
4. Endpoints    
   ```bash
    airflow -> http://localhost:8080/
    airflow username and password: admin
    control center -> http://localhost:9021/
    ```
5. Local Testing    
   ```bash
    docker ps # to get container names and id
    docker exec -it <webserver container id> airflow dags list  # list all the dags
    docker exec -it <webserver container id> airflow dags test <dag name> # to run dag
    ```
For more detailed instructions, please check out the video tutorial linked below.

## Deployement Approach
This Realtime Data ETL project orchestrates various components including Apache Airflow, PostgreSQL, Apache Kafka, Zookeeper, Control Center, Schema Registry, and Cassandra to handle stock market data. To leverage the scalability and robustness of container orchestration, deploying this project on Amazon Elastic Kubernetes Service (EKS) offers seamless management and resource allocation.
#### Deploying on Amazon EKS
- Convert the existing Docker Compose YAML file into Kubernetes resources
- Configure Kubernetes Services to enable communication between Docker containers across pods.
- An Amazon EKS cluster provisioned with appropriate IAM roles and permissions.
- Kubernetes CLI (kubectl) installed and configured to interact with the EKS cluster.
- Create a workflow file (e.g., .github/workflows/main.yml) in the project repository to define the CI/CD pipeline using YAML syntax.
- Configure triggers such as pushes to specific branches or pull requests to initiate the workflow execution.
## Watch the Video Tutorial

For a complete walkthrough and practical demonstration, check out [Loom Video Tutorial](https://www.loom.com/share/cf55704fd7014931a34667ea3dca3707?sid=f3769972-b3e6-4fe0-a6e2-944046708087).