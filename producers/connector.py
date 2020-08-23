"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    # check wether the connector exists
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return
    

    # Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    resp = requests.post(
        KAFKA_CONNECT_URL, 
        headers={"Content-Type": "application/json"}, 
        data=json.dumps({
            "name": CONNECTOR_NAME, 
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector", 
                "key.converter": "org.apache.kafka.connect.json.JsonConverter", 
                "key.converter.schemas.enable": "false", 
                "value.converter": "org.apache.kafka.connect.json.JsonConverter", 
                "value.converter.schemas.enable": "false", 
                "batch.max.rows": "500", 
                "connection.url": "jdbc:postgresql://localhost:5432/cta", 
                "connection.user": "cta_admin", 
                "connection.password": "chicago", 
                "table.whitelist": "stations", # Load the `stations` table 
                "mode": "incrementing", 
                "incrementing.column.name": "stop_id", 
                "topic.prefix": "com.transitchicago.", # topic name is going to be: <topic.prefix><table.whitelist>
                "poll.interval.ms": "60000", 
            }
        }),
    )

    ## Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        logger.error(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
    logging.info("connector created successfully")


if __name__ == "__main__":
    configure_connector()



## curl http://localhost:8083/connectors/<your_connector>/status to see what the status of your connector is

## curl http:<connect_url>/connectors/<your_connector>/tasks/<task_id>/status to see what the status of your task is

## show logs from kafka connect. 
## tail -f /var/log/journal/confluent-kafka-connect.service.log