#!/usr/bin/python3
import datetime
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

import Adafruit_DHT
from elasticsearch import Elasticsearch
from pydantic import BaseModel


@dataclass
class AppConfig(object):
    """
    Application Configuration data
    """
    es_host: str
    es_port: int
    es_index: str
    doc_tag: str
    pub_intvl: int
    gpio_pin: int
    sensor: int


class SensorReading(BaseModel):
    """
    Sensor reading data
    """
    hum_rh: float = 0
    temp_c: float = 0
    timestamp: datetime = None
    location: str = ""


SENSOR = Adafruit_DHT.DHT22

logging.basicConfig(level=logging.DEBUG)


def read_sensor(sensor: int, gpio_pin: int) -> (int, float, float, datetime):
    """
    Read from the sensor
    Args:
        sensor: The sensor to read from
        gpio_pin: The gpio pin where the sensor is plugged in.

    Returns: a tuple consisting of an RC (0 on success), Relative humidity, Temperature in C, and the current time.

    """
    logging.debug('reading sensor')
    hum_rh, temp_c = Adafruit_DHT.read_retry(sensor, gpio_pin)
    if hum_rh is None or temp_c is None:
        logging.error("failed to read from the sensor")
        return 1, 0, 0, datetime.now()
    logging.debug('sensor data: RH: {}, Tc: {}'.format(hum_rh, temp_c))
    return 0, hum_rh, temp_c, datetime.now()


def publish_data(es: Elasticsearch, doc_index: str, sensor_data: SensorReading) -> int:
    """
    Publish sensor data to ES
    Args:
        es: An ES context to use
        doc_index: the index to publish to
        sensor_data: a filled in SensorReading object to publish

    Returns: 0 on success

    """
    result = es.index(index=doc_index, doc_type='sensor_data', body=sensor_data.dict())
    if result.get("result","") != "created":
        logging.error("publishing error: result: {}".format(result))
        return 1
    return 0


def get_config() -> (int, Optional[AppConfig]):
    """
    Get configuration data from the environment variables set on process start
    Returns: an RC code (0 for success), and an AppConfig object if successful

    """
    es_host = os.environ.get("ES_HOST", "")
    if es_host == "":
        logging.error("empty env value for ES Host")
        return 1, None

    es_port_str = os.environ.get("ES_PORT", "")
    if es_port_str == "":
        logging.error("empty env value for ES PORT")
        return 1, None
    try:
        es_port = int(es_port_str)
    except ValueError as ve:
        logging.exception(
            "failed to convert es_port env var into int: {}".format(es_port), exc_info=ve)
        return 1, None

    es_index = os.environ.get("ES_INDEX", "")
    if es_index == "":
        logging.error("empty env value for ES INDEX")
        return 1, None

    doc_tag = os.environ.get("DOC_TAG", "")
    if doc_tag == "":
        logging.error("empty evn value for DOC_TAG")
        return 1, None

    interval_str = os.environ.get("PUB_INTVL", "")
    if doc_tag == "":
        logging.error("empty env value for PUB_INTVL")
        return 1, None
    try:
        interval = int(interval_str)
    except ValueError as ve:
        logging.exception("invalid value for PUB_INTVL: {}".format(interval_str),
                          exc_info=ve)
        return 1, None

    gpio_pin_str = os.environ.get("GPIO_PIN", "")
    if gpio_pin_str == "":
        logging.error("empty env value for GPIO_PIN")
        return 1, None
    try:
        gpio_pin = int(gpio_pin_str)
    except ValueError as ve:
        logging.exception(
            "failed to convert gpio pin env var to int: {}".format(gpio_pin_str),
            exc_info=ve)
        return 1, None

    app_cfg = AppConfig(es_host=es_host,
                        es_port=es_port,
                        es_index=es_index,
                        doc_tag=doc_tag,
                        pub_intvl=interval,
                        gpio_pin=gpio_pin,
                        sensor=SENSOR)
    return 0, app_cfg


def run(config: AppConfig) -> int:
    """
    Main method. Sets up elasticsearch context, runs infinite loop that gathers sensor
        data, formats it into an object, and publishes it to elasticsearch.
    Args:
        config: an initialized instance of AppConfig containing app configuration variables.

    Returns: 0 on successful exit

    """
    logging.info("starting")
    es = Elasticsearch(hosts=["{}:{}".format(config.es_host, config.es_port)])
    es.indices.create(index=config.es_index, ignore=400)

    while True:
        time.sleep(config.pub_intvl)
        rc, hum_rh, temp_c, ts = read_sensor(config.sensor, config.gpio_pin)
        if rc != 0:
            logging.error("failed to read from sensor")
            continue
        sensor_data = SensorReading(hum_rh=hum_rh, temp_c=temp_c, timestamp=ts,
                                    location=config.doc_tag)
        publish_data(es, config.es_index, sensor_data)

    return 0


if __name__ == "__main__":
    """
    Program entry point
    """
    rc, app_cfg = get_config()
    if rc != 0:
        logging.error("failed to get config")
        sys.exit(1)
    sys.exit(run(config=app_cfg))

# END OF FILE
