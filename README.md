# Realtime Data Streaming

## Intoduction
This project demonstrates a real-time data streaming pipeline, incorporating the extraction, transformation, and storage of data using industry-standard tools and technologies. The pipeline is built with Apache Kafka for data ingestion and streaming, Apache Spark for real-time data processing, Apache ZooKeeper for distributed coordination, Apache Cassandra for scalable and high-performance data storage, and Apache Airflow for workflow orchestration and pipeline automation.

## System Architecture
- **Data Source**: We use `randomuser.me` API to generate random user data for our pipeline.
- **Apache Airflow**: for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker
