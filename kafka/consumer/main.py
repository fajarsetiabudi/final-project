import logging
import argparse
import os
import time

from modules import consumer

def main(bootstrap_servers: str, topic: str, tablename: str, logger: logging.Logger):
    """
    Main Function
    Starting Consumer to ingest data from Kafka topics and load the data to database.
    """
    logger.info("Running Consumer")
    cns = consumer.Consumer(
        bootstrap_servers = bootstrap_servers,
        topic = topic,

        db_config = {
            # "host": "host.docker.internal",
            "host": "localhost",
            "port": "5432",
            "user": "postgres",
            "password": "admin"
        },
        tablename = tablename
    )
    cns.start()

    # Enable Process Termination
    try:
        while(True):
            time.sleep(60)
            logger.info("Consumer is running...")
    except KeyboardInterrupt:
        logger.info("Stopping Consumer")
        cns.stop()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--topic")
    parser.add_argument("--tablename")

    arguments = parser.parse_args()
    return {
        "bootstrap_servers": arguments.bootstrap_servers,
        "topic": arguments.topic,
        "tablename": arguments.tablename
    }

if __name__ == "__main__":
    logging.basicConfig(
        level = logging.INFO,
        format = "[ %(asctime)s ] { %(name)s } - %(levelname)s - %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )

    logger = logging.getLogger(__name__)

    arguments = get_args()
    main(**arguments, logger=logger)