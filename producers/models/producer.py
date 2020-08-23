"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092", 
            "schema.registry.url": "http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
        # See: https://docs.confluent.io/current/installation/configuration/topic-configs.html
        client = AdminClient({
            "bootstrap.servers": self.broker_properties["bootstrap.servers"]})
        
        exists = self.topic_exists(client, self.topic_name)
        logger.info(f"Topic {self.topic_name} exists: {exists}")
        if exists is True:
            return
        
        created_topics = client.create_topics(
            [
                NewTopic(topic=self.topic_name, 
                         num_partitions=self.num_partitions, 
                         replication_factor=self.num_replicas, 
                         config = {
                             "cleanup.policy": "delete",
                             "compression.type": "lz4"
                         }
                        )
            ]
        )
            
        for topic, future in created_topics.items():
            try:
                future.result()
                logger.info(f"topic {self.topic_name} created")
            except Exception as e:
                logger.info(f"failed to create topic {self.topic_name}: {e}")

    def topic_exists(self, client, topic_name):
        """Checks if the given topic exists"""
        # Check to see if the given topic exists
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.list_topics
        cluster_metadata = client.list_topics(timeout=5)
        return cluster_metadata.topics.get(topic_name) is not None

    def time_millis(self):
        return int(round(time.time() * 1000))

    def _unix_time_millis(self, dt):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return int((dt - epoch).total_seconds() * 1000.0)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # Write cleanup code for the Producer here
        if self.topic_name is not None:
            self.producer.flush()
            logger.info("producer close ...")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
