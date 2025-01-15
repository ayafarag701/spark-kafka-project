# Spark-Kafka Project

This project demonstrates how to use Apache Spark and Kafka for real-time data processing. The large CSV file used in this project can be downloaded from the link below:

Download CSV File: [Fitness tracker logs with user IDs and steps count.](https://www.kaggle.com/datasets/arnavsmayan/fitness-tracker-dataset)

## Features:
- Apache Kafka: Used for streaming data input.
- Apache Spark: Processes the real-time data.
- MySQL: Stores the aggregated data.

## How to Run:
1. Make sure Kafka and MySQL are running locally.
2. Update the database credentials and Kafka server settings as needed.
3. Run the producer script to send data to Kafka.
4. Run the consumer script to process the data in real-time and store it in MySQL.

## Technologies Used:
- Apache Kafka
- Apache Spark (PySpark)
- MySQL
- Python (Kafka-python, PySpark, MySQL Connector)
