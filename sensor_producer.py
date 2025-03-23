touch sensor_producer.py 

from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

def create_sensor_data():
    # Замініть 'ihor' на той самий ідентифікатор, що і в create_topics.py
    your_name = "ihor"
    
    # Ініціалізація продюсера
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    # Генерація унікального ID датчика для цього запуску
    sensor_id = random.randint(1000, 9999)
    print(f"Запущено датчик з ID: {sensor_id}")
    
    # Нескінченний цикл для генерації даних
    try:
        while True:
            # Генерація випадкових даних
            temperature = round(random.uniform(25, 45), 1)
            humidity = round(random.uniform(15, 85), 1)
            
            data = {
                "sensor_id": sensor_id,
                "timestamp": datetime.now().isoformat(),
                "temperature": temperature,
                "humidity": humidity
            }
            
            # Відправка даних у топік
            producer.send(f'{your_name}_building_sensors', value=data)
            print(f"Відправлено дані: Температура: {temperature}°C, Вологість: {humidity}%")
            
            # Пауза між відправками
            time.sleep(5)
    except KeyboardInterrupt:
        print("Датчик зупинено")

if __name__ == "__main__":
    create_sensor_data()
