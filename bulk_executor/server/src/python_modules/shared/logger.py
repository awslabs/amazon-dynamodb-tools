import logging

# Set all external loggers to WARNING level by default
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('py4j').setLevel(logging.WARNING)

log = None

def init(args=None):
    if args is None:
        args = {}

    global log
    
    # Configure root logger
    root_logger = logging.getLogger()
    
    # Remove default handlers
    root_logger.handlers.clear()
    
    # Set log level based on XDebug flag
    if args.get('XDebug'):
        root_logger.setLevel(logging.DEBUG)
    else:
        root_logger.setLevel(logging.INFO)

    log_stream_handler = logging.StreamHandler()
    log_stream_handler.setFormatter(BulkDynamoDBServerSideFormatter())
    root_logger.addHandler(log_stream_handler)

    log = logging  # File scope assignment intentional

    # log.debug("DEBUG LOGS ENABLED SERVER SIDE") # Client and Server side should be in sync for all log levels. Uncomment if needed.

class BulkDynamoDBServerSideFormatter(logging.Formatter):
    full_format='%(asctime)s %(levelname)-5s [%(threadName)s] %(name)s - %(message)s'
    info_format='%(message)s'

    FORMATS = {
        logging.DEBUG: full_format,
        logging.INFO: info_format,
        logging.WARNING: full_format,
        logging.ERROR: full_format,
        logging.CRITICAL: full_format,
    }

    def __init__(self):
        super().__init__()
        self.formatters = {
            logging.DEBUG: logging.Formatter(self.FORMATS[logging.DEBUG]),
            logging.INFO: logging.Formatter(self.FORMATS[logging.INFO]),
            logging.WARNING: logging.Formatter(self.FORMATS[logging.WARNING]),
            logging.ERROR: logging.Formatter(self.FORMATS[logging.ERROR]),
            logging.CRITICAL: logging.Formatter(self.FORMATS[logging.CRITICAL])
        }

    def format(self, record):
        formatter = self.formatters.get(record.levelno)
        return formatter.format(record)

init()
