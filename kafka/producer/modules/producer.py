import logging
import requests
import threading
import time
import json
from datetime import datetime
from random import SystemRandom
from kafka import KafkaProducer



class ProducerThread(threading.Thread):
    """
    Producer in Thread
    Producer function that run in single thread, allowing multithread runtime.
    Inherited from Thread.
    params:
    - name: str. Thread process name (label).
    - args: tuple. Arguments used by producer function.
    - bootstrap_servers: str. Kafka instance hostname (bootstrap).
    - topic: str. Topic for producer to publish data.
    """
    def __init__(self, name: str, args: tuple, bootstrap_servers: str, topic: str):
        super().__init__(
            target = self._produce,
            name   = name,
            args   = args
        )
        self.active = False
        
        self._setup_publisher(bootstrap_servers, topic)
        self.logger = logging.getLogger(__name__)


    # Public Methods
    def start(self):
        """
        Start Producer
        Starting producer activity.
        """
        self.active = True
        super().start()

    def stop(self):
        """"
        Stop Producer
        Stopping producer activity.
        """
        self.active = False

    def get_stream(self, url):
        s = requests.Session()

        with s.get(url, headers=None, stream=True) as resp:
            for line in resp.iter_lines(decode_unicode=True):
                if line:
                    return line


    # Private Methods
    def _setup_publisher(self, bootstrap_servers, topic):
        kafka_logger = logging.getLogger("kafka.conn")
        kafka_logger.setLevel(logging.ERROR)
        
        self.publisher = KafkaProducer(
            value_serializer = self._serializer,
            bootstrap_servers = bootstrap_servers
        )
        self.topic = topic
    
    def _serializer(self, data: dict):
        return json.dumps(data).encode("utf-8")

    def _produce(self, id: int):
        url = 'https://www.freeforexapi.com/api/live?pairs=EURUSD,EURGBP,USDEUR'

        while (self.active):
            res_data = self.get_stream(url)
 
            self.logger.info(f"Producer {id:2}: Create data with data: {res_data}")

            self._publish(res_data)
            time.sleep((60))

    def _publish(self, data: dict):
        future = self.publisher.send(self.topic, data)
        future.get(timeout = 5)