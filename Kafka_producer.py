from kafka import KafkaProducer
import csv

def produce_to_kafka(input_path, kafka_topic, bootstrap_servers='localhost:9092'):
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.encode('utf-8'))
        
        with open(input_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  

            for row in reader:
                message = f"{row[0].strip()},{row[2].strip()}"
                
                try:
                    producer.send(kafka_topic, value=message)
                    print(f"Sent message: {message}") 
                except Exception as e:
                    print(f"Error sending message: {message}, Error: {str(e)}")

        producer.flush()
        producer.close()
        print("All messages sent successfully.")  

    except Exception as e:
        print(f"Error in Kafka producer: {str(e)}")

input_path = "D:/Shared Folder/fitness_tracker_dataset.csv"
kafka_topic = "user_steps"

produce_to_kafka(input_path, kafka_topic)
