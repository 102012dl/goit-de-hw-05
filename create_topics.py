touch create_topics.py 

from kafka.admin import KafkaAdminClient, NewTopic

def create_topics():
    # Замініть 'ihor' на своє ім'я або інший унікальний ідентифікатор
    your_name = "ihor"
    
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092"
    )
    
    topic_list = [
        NewTopic(name=f"{your_name}_building_sensors", num_partitions=1, replication_factor=1),
        NewTopic(name=f"{your_name}_temperature_alerts", num_partitions=1, replication_factor=1),
        NewTopic(name=f"{your_name}_humidity_alerts", num_partitions=1, replication_factor=1)
    ]
    
    try:
        admin_client.create_topics(new_topics=topic_list)
        print(f"Топіки успішно створені:")
        for topic in topic_list:
            print(f"- {topic.name}")
    except Exception as e:
        print(f"Помилка при створенні топіків: {e}")

if __name__ == "__main__":
    create_topics()
