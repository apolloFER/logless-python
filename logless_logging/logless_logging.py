import datetime
import uuid
import logging
import msgpack

import logless_logging.producer


def level_converter(level):
    return {logging.CRITICAL: "error",
            logging.ERROR: "error",
            logging.WARNING: "warning",
            logging.INFO: "info",
            logging.DEBUG: "debug"
            }.get(level, "")


class _LogLessFormatter(logging.Formatter):
    def __init__(self, hostname, extra=None):
        super(_LogLessFormatter, self).__init__()
        self.hostname = hostname
        self.extra = extra

    def formatMessage(self, record):
        fields = record.__dict__.get(u'extra', None)

        if self.extra is not None:
            if fields is None:
                fields = self.extra
            else:
                fields.update(self.extra)

        data = {
            "message": record.msg,
            "timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "level": level_converter(record.levelno),
            "source": self.hostname,
            "fields": fields
        }

        return msgpack.dumps(data, use_bin_type=True)

    def format(self, record):
        """
        Format the specified record as text.

        The record's attribute dictionary is used as the operand to a
        string formatting operation which yields the returned string.
        Before formatting the dictionary, a couple of preparatory steps
        are carried out. The message attribute of the record is computed
        using LogRecord.getMessage(). If the formatting string uses the
        time (as determined by a call to usesTime(), formatTime() is
        called to format the event time. If there is exception information,
        it is formatted using formatException() and appended to the message.
        """
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        s = self.formatMessage(record)
        return s


class LogLessHandler(logging.Handler):
    def __init__(self, stream_name):
        super(LogLessHandler, self).__init__()
        self.producer = logless_logging.producer.KinesisProducer(stream_name=stream_name)

    def emit(self, record):
        data = self.format(record)
        self.producer.add_record(str(uuid.uuid4()), data)

    def flush(self):
        pass

    def close(self):
        super(LogLessHandler, self).close()
        self.producer.close()


def create_handler(stream_name, hostname):
    handler = LogLessHandler(stream_name)
    handler.setFormatter(_LogLessFormatter(hostname))

    return handler
