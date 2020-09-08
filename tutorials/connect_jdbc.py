import asyncio
import json

import requests


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "clicks-jdbc"

def configure_connector():
    """Calls Kafka Connect to create the Connector"""
    print("creating or updating kafka connect connector...")

	rest_method = requests.post
	
    # check wether connector exists
	resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    #
    # Complete the Kafka Connect Config below for a JDBC source connector.
    #       You should whitelist the `clicks` table, use incrementing mode and the
    #       incrementing column name should be id.
    #
    #       See: https://docs.confluent.io/current/connect/references/restapi.html
    #       See: https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html
    #
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": "clicks-jdbc", # connector_name
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector", 
                    "topic.prefix": "solution3.",  # topic name is going to be: <topic.prefix><table.whitelist>
                    "mode": "incrementing", 
                    "incrementing.column.name": "id", 
                    "table.whitelist": "clicks",  # table name
                    "tasks.max": 1,
                    "connection.url": "jdbc:postgresql://localhost:5432/classroom", 
                    "connection.user": "root",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            }
        ),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    print("connector created successfully.")
    print("Use kafka-console-consumer and kafka-topics to see data!")


if __name__ == "__main__":
    configure_connector()


## curl http://localhost:8083/connectors/<your_connector>/status to see what the status of your connector is

## curl http:<connect_url>/connectors/<your_connector>/tasks/<task_id>/status to see what the status of your task is

## show logs from kafka connect. 
## tail -f /var/log/journal/confluent-kafka-connect.service.log