import os

from kafka import KafkaConsumer
import json
import pickle
from dotenv import load_dotenv
import os
load_dotenv()

# Initialize KafkaConsumer
consumer = KafkaConsumer(
    'my_topic',
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    group_id='my_group',
    value_deserializer=lambda x: pickle.loads(x),
)

count = 0
is_first_message = True

# Open the result.json file for appending
with open("result.json", "a") as file:
    file.write("[\n")  # Start of JSON array

    for message in consumer:
        if message.key:
            key = json.dumps(message.key)
        else:
            key = None
        value = json.dumps(message.value)

        # Handle commas between key-value pairs
        if not is_first_message:
            file.write(",\n")  # Add a comma before the next object
        else:
            is_first_message = False  # Mark first message as handled

        # Write the key-value pair as JSON
        if key is None:
            file.write(f"{value}")
        else:
            file.write(f'{{"key": {key}, "value": {value}}}')

        # Print the result to the console
        print(f"{key}: {value}")
        count += 1
        if count == 3:
            break
        print(count)

    file.write("\n]")  # End of JSON array

print("Consumer complete successfully")
