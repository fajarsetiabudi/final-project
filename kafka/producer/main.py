import logging
import argparse
import time

from modules import producer

def main(worker: int, bootstrap_servers: str, topic: str, logger: logging.Logger):
    """
    Main Function
    Starting Producer to create dummy transaction funnel and send the data to Kafka topics.
    """
    logger.info(f"Running {worker} Producer. Topic '{topic}' in '{bootstrap_servers}'")
    producer_list = [
        producer.ProducerThread(
            name = f"producer_{i:02}",
            args = (i,),
            bootstrap_servers = bootstrap_servers,
            topic = topic
        )
        for i in range(worker)
    ]
    for prd in producer_list:
        prd.start()
        time.sleep(1)
    
    # Enable Process Termination
    try:
        while(True):
            time.sleep(60)
            logger.info("Producer is running...")
    except KeyboardInterrupt:
        logger.info("Stopping Producer")
        for prd in producer_list:
            prd.stop()
            prd.join()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", type=int)
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--topic")

    arguments = parser.parse_args()
    return {
        "worker": arguments.worker,
        "bootstrap_servers": arguments.bootstrap_servers,
        "topic": arguments.topic
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
