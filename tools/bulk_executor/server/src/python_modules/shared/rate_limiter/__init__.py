from .DistributedDynamoDBMonitorAggregator import DistributedDynamoDBMonitorAggregator
from .DistributedDynamoDBMonitorWorker import DistributedDynamoDBMonitorWorker

from boto3 import Session
from ..logger import log


class RateLimiterSharedConfig:
    """
    Configuration for Bulk DynamoDB Rate Limiting.
    
    This class encapsulates the shared configuration between worker and aggregator instances ensuring assigned values
    are consistent between both Aggregator and Worker.
    """
    def __init__(self, bucket, job_run_id):
        """
        Initialize the configuration.
        
        Args:
            bucket (str): The S3 bucket name for rate limiter reporting
            rate_limiter_prefix (str): The S3 prefix for rate limiter reporting
        """
        self.bucket = bucket
        self.prefix = f"server/rate-limiter/{job_run_id}"


class RateLimiterAggregator:
    """
    Wraps the DistributedDynamoDBMonitorAggregator to ensure safe Bulk DynamoDB Session handling and throughput configurations.

    Args:
        shared_config (RateLimiterSharedConfig): The shared config between Aggregator and Worker.
        modes (none to many list of ("read", "write")): The expected execution modes of the DynamoDB actions requiring rate limiting.
    """
    def __init__(self, shared_config):
        log.info(f"Initializing...Bucket:{shared_config.bucket}, Prefix:{shared_config.bucket}")

        self.rate_limiter_monitor_aggregator = DistributedDynamoDBMonitorAggregator(
            session=Session(),
            bucket=shared_config.bucket,
            prefix=shared_config.prefix,
        )

    def shutdown(self):
        log.info("Shutting down... Cleaning up rate limit aggregator")
        self.rate_limiter_monitor_aggregator.cleanup()


class RateLimiterWorker:
    """
    Wraps the DistributedDynamoDBMonitorWorker to ensure safe Bulk DynamoDB Session handling and throughput configurations.
    The worker dynamodb_client should be use for any actions against DDB that should be leveraging the rate limiter.

        Warning: This must be instantiated within the worker execution.

    Args:
        shared_config (RateLimiterSharedConfig): The shared config between Aggregator and Worker.
        monitor_options: The expected monitor options (see @table_info#get_dynamodb_throughput_configs for more info)
        debug_accumulator (DebugAccumulator): The debug accumulator to use for rate limiter
    """
    def __init__(self, shared_config, debug_accumulator=None, **monitor_options):
        self.session = Session()
        log.info(f"Rate limiter, init, monitor_options {monitor_options}")
        if debug_accumulator:
            debug_accumulator.add([f"RateLimiterWorker init: monitor_options={monitor_options}"])
        self.rate_limiter_monitor_worker = DistributedDynamoDBMonitorWorker(
            session=self.session,
            bucket=shared_config.bucket,
            prefix=shared_config.prefix,
            debug_accumulator=debug_accumulator,
            **monitor_options
        )

    def get_session(self):
        return self.session

    def shutdown(self):
        log.info("Shutting down... Cleaning up rate limit worker.")
        self.rate_limiter_monitor_worker.cleanup()
