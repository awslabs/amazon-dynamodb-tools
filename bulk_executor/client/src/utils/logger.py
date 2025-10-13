import logging

# Set all external loggers to WARNING level by default
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)

class ColorCodes():
    BLUE = "\x1b[34m"
    BOLD_RED = "\x1b[31;1m"
    GRAY = "\x1b[90m"
    GREEN = "\x1b[92m"
    RED = "\x1b[31;20m"
    RESET = "\x1b[0m"
    YELLOW = "\x1b[33;20m"
    PINK = "\x1b[91m"


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

    # Create colored stream handler
    colored_handler = logging.StreamHandler()
    colored_handler.setFormatter(ColoredFormatter())
    root_logger.addHandler(colored_handler)

    log = logging  # File scope assignment intentional

    log.debug("DEBUG LOGS ENABLED") # Notify debug logs enabled

class ColoredFormatter(logging.Formatter):
    full_format='%(asctime)s %(levelname)-5s [%(threadName)s] %(name)s - %(message)s'
    info_format='%(message)s'

    FORMATS = {
        logging.DEBUG: ColorCodes.GREEN + full_format + ColorCodes.RESET,
        logging.INFO: ColorCodes.RESET + info_format + ColorCodes.RESET,
        logging.WARNING: ColorCodes.YELLOW + full_format + ColorCodes.RESET,
        logging.ERROR: ColorCodes.PINK + full_format + ColorCodes.RESET,
        logging.CRITICAL: ColorCodes.BOLD_RED + full_format + ColorCodes.RESET
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
