from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
# from kafka.vendor import six
# from six.moves import range

# Initialize Kafka Admin Client
admin_client = KafkaAdminClient(bootstrap_servers='localhost:29092')

# List of topics to create
topics = ['orders_input', 'matched_trades', 'analytics_output']

# Create topics
try:
    admin_client.create_topics(
        [NewTopic(name=topic, num_partitions=1, replication_factor=1) for topic in topics]
    )
    print("Kafka topics created successfully!")
except TopicAlreadyExistsError:
    print("Some or all topics already exist.")
except Exception as e:
    print(f"An error occurred: {e}")
