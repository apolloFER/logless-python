import threading
import atexit
import boto3
import time
import uuid

from concurrent.futures import ThreadPoolExecutor
from kinaggregator import aggregator


def encode_data(data, encoding='utf_8'):
    if isinstance(data, bytes):
        return data
    else:
        return str(data).encode(encoding)


class KinesisProducer:
    def __init__(self, stream_name, batch_size=500, batch_time=1, kinesis_client=boto3.client('kinesis')):
        self.stream_name = stream_name
        self.batch_size = batch_size
        self.batch_time = batch_time
        self.kinesis_client = kinesis_client
        self.aggregator = aggregator.RecordAggregator()
        self.aggregator.on_record_complete(self.record_complete, execute_on_new_thread=False)
        self.pool = ThreadPoolExecutor(2)
        self.last_flush = time.time()
        self.monitor_running = threading.Event()
        self.monitor_running.set()
        self.pool.submit(self.monitor)

        atexit.register(self.close)

    def monitor(self):
        while self.monitor_running.is_set():
            if time.time() - self.last_flush > self.batch_time:
                try:
                    self.pool.submit(self.flush)
                except RuntimeError:
                    break

                time.sleep(self.batch_time / 10.0)

    def put_record(self, data, partition_key=None):
        # Byte encode the data
        data = encode_data(data)

        # Create a random partition key if not provided
        if not partition_key:
            partition_key = uuid.uuid4().hex

        self.pool.submit(self.add_record, partition_key, data)

    def add_record(self, partition_key, record):
        self.aggregator.add_user_record(partition_key.encode(), record)

        if self.aggregator.get_num_user_records() >= self.batch_size:
            self.flush()

    def flush(self):
        if self.aggregator.get_num_user_records() > 0:
            self.record_complete(self.aggregator.clear_and_get())

    def record_complete(self, record):
        pk, ehk, data = record.get_contents()

        try:
            self.kinesis_client.put_record(StreamName=self.stream_name,
                                           Data=data,
                                           PartitionKey=pk,
                                           ExplicitHashKey=ehk)
        except Exception as e:
            pass
        else:
            self.last_flush = time.time()

    def close(self):
        """Flushes the queue and waits for the executor to finish."""
        if self.monitor_running.is_set():
            self.monitor_running.clear()
            self.pool.shutdown()
            self.flush()
