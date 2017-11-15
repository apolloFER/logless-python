# LogLess
> LogLess is a centralized logging/event stack using AWS Kinesis/Lambda. The Python version works as a logging Handler.

## Usage

Use the `create_handler` function to create a new `LogLessHandler` with the name of your Kinesis stream as the first parameter. The second parameter is the hostname of your machine.


## Example


```python
import logless_logging
import logging
import socket
import time

logger = logging.getLogger()

if __name__ == '__main__':
    handler = logless_logging.create_handler("logless-test", socket.gethostname())
    logger.addHandler(handler)

    logger.info("Testing 123")
    logger.warning("Testing 124")
    logger.error("Testing 125")
    
    time.sleep(2)
```


### License
MIT License
