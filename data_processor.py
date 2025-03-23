touch data_processor.py 

from kafka import KafkaConsumer, KafkaProducer
import json

def process_sensor_data():
    # Замініть 'ihor' на той самий ідентифікатор, що і в create_topics.py
    your_name = "ihor"
    
    # Ініціалізація споживача для отримання даних з датчиків
    consumer = KafkaConsumer(
        f'{your_name}_building_sensors',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Ініціалізація продюсера для відправки сповіщень
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    print(f"Процесор даних запущено. Очікування повідомлень...")
    
    # Основний цикл обробки повідомлень
    for message in consumer:
        data = message.value
        sensor_id = data['sensor_id']
        temperature = data['temperature']
        humidity = data['humidity']
        timestamp = data['timestamp']
        
        print(f"Отримано дані від датчика {sensor_id}: Темп.: {temperature}°C, Волог.: {humidity}%")
        
        # Перевірка температури
        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "message": "Увага! Температура перевищує допустимий рівень!"
            }
            producer.send(f'{your_name}_temperature_alerts', value=alert)
            print(f"СПОВІЩЕННЯ: Висока температура {temperature}°C від датчика {sensor_id}!")
        
        # Перевірка вологості
        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "message": "Увага! Рівень вологості поза допустимими межами!"
            }
            producer.send(f'{your_name}_humidity_alerts', value=alert)
            print(f"СПОВІЩЕННЯ: Критична вологість {humidity}% від датчика {sensor_id}!")

if __name__ == "__main__":
    process_sensor_data()
