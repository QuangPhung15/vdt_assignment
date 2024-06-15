import pandas as pd
import time
import datetime
from kafka import KafkaProducer
import json

# Define Kafka topic and broker
KAFKA_TOPIC = "vdt2024"
KAFKA_BROKER = "localhost:9092"  # Change this to your Kafka broker address

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read CSV file
with open('log_action.csv', mode='r') as file:
    for row in file:
        row = row.strip()
        col = row.split(",")
        message = {
            "student_code": int(col[0]),
            "activity": col[1],
            "numberOfFile": int(col[2]),
            "timestamp": col[3]
        }
        
        # Send the message to Kafka
        producer.send(KAFKA_TOPIC, value=message)
        
        # Print the message for debug purposes
        print(f"Sent: {message}")
        
        # Sleep for 1 second
        time.sleep(1)

# Close the producer
producer.close()
