touch alert_consumer.py 

from kafka import KafkaConsumer
import json
from datetime import datetime

def consume_alerts():
    # Замініть 'ihor' на той самий ідентифікатор, що і в create_topics.py
    your_name = "ihor"
    
    # Ініціалізація споживача для отримання сповіщень
    consumer = KafkaConsumer(
        f'{your_name}_temperature_alerts',
        f'{your_name}_humidity_alerts',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Консумер сповіщень запущено. Очікування сповіщень...")
    
    # Основний цикл отримання сповіщень
    for message in consumer:
        alert = message.value
        topic = message.topic
        
        # Визначення типу сповіщення за назвою топіку
        if "temperature" in topic:
            print("\n" + "="*50)
            print(f"[ТЕМПЕРАТУРНЕ СПОВІЩЕННЯ]")
            print(f"Датчик ID: {alert['sensor_id']}")
            print(f"Температура: {alert['temperature']}°C")
            print(f"Час: {alert['timestamp']}")
            print(f"Повідомлення: {alert['message']}")
            print("="*50)
        elif "humidity" in topic:
            print("\n" + "="*50)
            print(f"[СПОВІЩЕННЯ ПРО ВОЛОГІСТЬ]")
            print(f"Датчик ID: {alert['sensor_id']}")
            print(f"Вологість: {alert['humidity']}%")
            print(f"Час: {alert['timestamp']}")
            print(f"Повідомлення: {alert['message']}")
            print("="*50)

if __name__ == "__main__":
    consume_alerts()
