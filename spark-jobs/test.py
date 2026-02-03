from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'weather_raw',
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Listening...")

try:
    for message in consumer:
        print(f"Received: {message.value}")
except:
    print("\nStopping")

consumer.close()