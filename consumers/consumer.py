"""Defines core consumer functionality"""
import json
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094",
            "schema.registry.url": "http://localhost:8081"
        }

        # Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.consumer = AvroConsumer(
                {
                    "bootstrap.servers": self.broker_properties["bootstrap.servers"],
                    "schema.registry.url": self.broker_properties["schema.registry.url"],
                    "group.id": self.topic_name_pattern,
                    "auto.offset.reset": "earliest"
                }
            )
        else:
            self.consumer = Consumer(
                {
                    "bootstrap.servers": self.broker_properties["bootstrap.servers"],
                    "group.id": self.topic_name_pattern,
                    "auto.offset.reset": "earliest"
                }
            )

        # Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        message = self.consumer.poll(timeout=self.consume_timeout)
        if message is None:
            logger.info("No message consumed")
        elif message.error() is not None:
            logger.error("Error in consumed message", exc_info=message.error())
        else:
            logger.info(f"Consuming message from topic {message.topic()}\n{message.key()}:{message.value()}")
            self.message_handler(message)
            return 1
        return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
