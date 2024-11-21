from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, my_name
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    # security_protocol=kafka_config['security_protocol'],
    # sasl_mechanism=kafka_config['sasl_mechanism'],
    # sasl_plain_username=kafka_config['username'],
    # sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    # security_protocol=kafka_config['security_protocol'],
    # sasl_mechanism=kafka_config['sasl_mechanism'],
    # sasl_plain_username=kafka_config['username'],
    # sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
topic_name = f'{my_name}_building_sensors'

# Підписка на тему
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
        if message.value["temp"] > 40:
            data = message.value.copy()
            data['message'] = f"Temperature is too high: {message.value['temp']} for sensor with key: {message.key}"
            producer.send(f'{my_name}_temperature_alerts', key=message.key, value=data)
            producer.flush()
        #     print(f"Temperature is too high: {message.value['temp']} for sensor with key: {message.key}")
        if message.value["humidity"] < 20:
            data = message.value.copy()
            data['message'] = f"Humidity is too low: {message.value['humidity']} for sensor with key: {message.key}"
            producer.send(f'{my_name}_humidity_alerts', key=message.key, value=data)
            producer.flush()
        #     print(f"Humidity is too low: {message.value['humidity']} for sensor with key: {message.key}")
        if message.value["humidity"] > 80:
            data = message.value.copy()
            data['message'] = f"Humidity is too high: {message.value['humidity']} for sensor with key: {message.key}"
            producer.send(f'{my_name}_humidity_alerts', key=message.key, value=data)
            producer.flush()
        #     print(f"Humidity is too high: {message.value['humidity']} for sensor with key: {message.key}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer