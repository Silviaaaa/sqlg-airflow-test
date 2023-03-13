# kafka_example_dag_1.py 

import os
import json
import logging
import functools
from pendulum import datetime

from airflow import DAG
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator

# get the topic name from .env
# my_topic = "airflow_test"
my_topic = os.environ["KAFKA_TOPIC_NAME"]

connection_config = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
    "security.protocol": os.environ["SECURITY_PROTOCOL"],
    "sasl.mechanism": "PLAIN",
    "sasl.username": None,
    "sasl.password": None
}

with DAG(
    dag_id="kafka_dag_example",
    start_date=datetime(2022, 11, 1),
    schedule=None,
    catchup=False,
):

    # define the producer function
    def producer_function():
        for i in range(5):
            yield (json.dumps(i), json.dumps(i+1))

    # define the consumer function
    def consumer_function(message, prefix=None):
        try:
            key = json.loads(message.key())
            value = json.loads(message.value())
            consumer_logger.info(f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}")
            return
        except:
            consumer_logger.info(f"Unable to consume message!")
            return

    # define the producer task
    producer_task = ProduceToTopicOperator(
        task_id=f"produce_to_{my_topic}",
        topic=my_topic,
        producer_function=producer_function, 
        kafka_config=connection_config
    )

    # define the consumer task
    consumer_logger = logging.getLogger("airflow")
    consumer_task = ConsumeFromTopicOperator(
        task_id=f"consume_from_{my_topic}",
        topics=[my_topic],
        apply_function=functools.partial(consumer_function, prefix="consumed:::"),
        consumer_config={
            **connection_config,
            "group.id": "consume",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        max_messages=30,
        max_batch_size=10,
        poll_timeout=10,
    )
    
    # define DAG
    producer_task >> consumer_task