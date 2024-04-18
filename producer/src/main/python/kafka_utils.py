from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
import socket
from dotenv import load_dotenv
import os
load_dotenv()


conf = {'bootstrap.servers': os.getenv("BOOTSTRAP_SERVER"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv("CONFLUENT_API"),
        'sasl.password': os.getenv("CONFLUENT_API_SECRET"),
        'client.id': socket.gethostname()}


producer = Producer(conf)
topics = AdminClient(conf).list_topics().topics