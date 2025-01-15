import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
import mysql.connector

connection = mysql.connector.connect(
    host="localhost",         
    user="root",              
    password="0000"  
)
cursor = connection.cursor()

cursor.execute("DROP DATABASE IF EXISTS user_steps_db")
print("Old database removed.")

cursor.execute("CREATE DATABASE user_steps_db")
print("New database created.")

cursor.close()
connection.close()

spark = SparkSession.builder.appName("KafkaConsumerMySQL").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_steps") \
    .load() \
    .selectExpr("CAST(value AS STRING)")  

df = df.withColumn("user_id", split(col("value"), ",")[0]) \
       .withColumn("steps", split(col("value"), ",")[1].cast("int"))

aggregated_df = df.groupBy("user_id").sum("steps").withColumnRenamed("sum(steps)", "total_steps")

def write_to_mysql(batch_df, batch_id):
    try:
        connection = mysql.connector.connect(
            host="localhost",         
            user="root",              
            password="0000", 
            database="user_steps_db"  
        )
        cursor = connection.cursor()

        cursor.execute(''' 
            CREATE TABLE IF NOT EXISTS user_step (
                user_id VARCHAR(255) PRIMARY KEY, 
                total_steps INT
            )
        ''')

        for row in batch_df.collect():
            print(f"Storing data for user_id: {row.user_id}, total_steps: {row.total_steps}")  
            cursor.execute("SELECT total_steps FROM user_step WHERE user_id = %s", (row.user_id,))
            existing_row = cursor.fetchone()

            if existing_row:
                cursor.execute("UPDATE user_step SET total_steps = %s WHERE user_id = %s", (row.total_steps, row.user_id))
                print(f"Updated user_id: {row.user_id} with total_steps: {row.total_steps}")  
            else:
                cursor.execute("INSERT INTO user_step (user_id, total_steps) VALUES (%s, %s)", (row.user_id, row.total_steps))
                print(f"Inserted new user_id: {row.user_id} with total_steps: {row.total_steps}") 

        connection.commit()

    except mysql.connector.Error as e:
        print(f"Error occurred while accessing MySQL database: {e}")

    finally:
        if connection:
            connection.close()

query = aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .start()

query.awaitTermination()
