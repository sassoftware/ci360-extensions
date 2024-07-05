"""
Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import configparser
import logging
from logging.handlers import RotatingFileHandler
from confluent_kafka import Consumer, KafkaError
import requests
import json
import os
import base64
import jwt
import threading

# Get the directory of the current script
# Set the working directory to the script's directory
script_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_dir)


# Set up logging
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Rotating log file handler
log_file = os.path.join(script_dir, 'script_log.log')
file_handler = RotatingFileHandler(log_file, maxBytes=100*1024*1024, backupCount=5)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)



def get_bearer_token(tenantId, secret):
    encodedSecret = base64.b64encode(bytes(secret, 'utf-8'))
    token = jwt.encode({'clientID': tenantId}, encodedSecret, algorithm='HS256')
    logger.info('JWT token generated: %s', token)

    return token


def send_to_ci360(event, url, token, default_event_name):
    headers = {
        'Content-type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    # Check if the event has 'eventName', if not, add the default event name
    if not any(key.lower() == 'eventname' for key in event):
        event['eventName'] = default_event_name

    try:
        logger.info("Processed Event: %s", event)
        response = requests.post(url, headers=headers, json=event)
        response.raise_for_status()  # Raise an exception for HTTP errors
        logger.info("Response from CI360: %s", response.text)
    except requests.exceptions.RequestException as e:
        logger.error("Error sending event to CI360: %s", e)


def apply_field_mapping(event, field_mapping):
    mapped_event = {}
    for key, path in field_mapping.items():
        value = event
        for step in path.split('.'):
            if isinstance(value, dict) and step in value:
                value = value[step]
            elif isinstance(value, list) and step.isdigit() and int(step) < len(value):
                value = value[int(step)]
            else:
                value = None
                break
        if value is not None:
            mapped_event[key] = value
    return mapped_event


def consume_from_kafka(url, token, bootstrap_servers, groupid, topic, field_mapping, default_event_name):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': groupid,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(msg.error())
                    break

            event = json.loads(msg.value().decode('utf-8'))
            logger.info("Received message: %s", event)
            mapped_event = apply_field_mapping(event, field_mapping)

            # Introduce threading for parallel processing
            thread = threading.Thread(target=send_to_ci360, args=(mapped_event, url, token, default_event_name))
            thread.start()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')  # Update with your config file name and path
    domain = config['CI360']['url']
    url = r"https://" + domain + r"/marketingGateway/events"

    tenantID = config['CI360']['tenantID']
    clientSecret = config['CI360']['clientSecret']

    token = get_bearer_token(tenantID, clientSecret)

    bootstrap_servers = config['Kafka']['bootstrap_servers']
    topic = config['Kafka']['topic']
    groupid = config['Kafka']['groupid']
    default_event_name = config['CI360']['default_event_name']

    field_mapping = {}
    field_config = configparser.ConfigParser()
    field_config.read('field_mapping.ini')  # Update with your field mapping config file name and path

    for section in field_config.sections():
        field_mapping.update(field_config[section])

    consume_from_kafka(url, token, bootstrap_servers, groupid , topic, field_mapping, default_event_name)
