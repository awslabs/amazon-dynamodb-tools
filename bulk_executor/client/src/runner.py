import json
import sys
import threading
import time
from datetime import datetime

import utils
from botocore.exceptions import ClientError
# project files
from clients import Clients
from infrastructure import GLUE_JOB_NAME, GlueJobDefaults
from infrastructure.constants import GLUE_LOG_GROUP_ERROR, GLUE_LOG_GROUP_OUTPUT
from infrastructure.verifier import assert_version_parity, is_existing_glue_job
from reassembler import GlueLogReassembler
from utils.graceful_interrupt_handler import GracefulInterruptHandler
from utils.logger import ColorCodes, log

# Constants
FAILED_STATE = 'FAILED'
RUNNING_STATE = 'RUNNING'
STOPPED_STATE = 'STOPPED'
STOPPING_STATE = 'STOPPING'
SUCCEEDED_STATE = 'SUCCEEDED'
TIMEOUT_STATE = 'TIMEOUT'

LIVE_TAIL_MAX_RETRIES = 20
LIVE_TAIL_RETRY_WAIT_TIME_IN_SECONDS = 2

# Logs are streamed every second, so no need for a wait timer (count alone is sufficient).
LIVE_TAIL_SUCCESS_SHUTDOWN_MAX_COUNT = 3 # Complete the job after this many counts of no additional logs coming through.

TERMINAL_JOB_STATES = set([
    STOPPED_STATE,
    FAILED_STATE,
    TIMEOUT_STATE
])

# Timing constants
WAIT_TIME_SECONDS = 0.5 # Time to wait before checking job run state again (in seconds)

class BulkDynamoDbRunner:
    def __init__(self, env_configs):
        self.aws_region = env_configs.aws_region
        self.aws_account_id = env_configs.aws_account_id

        clients = Clients(self.aws_region)
        self.dynamodb_client = clients.dynamodb_client
        self.glue_client = clients.glue_client
        self.logs_client = clients.logs_client

    def _get_log_group_arns(self):
        return [
            f"arn:aws:logs:{self.aws_region}:{self.aws_account_id}:log-group:{GLUE_LOG_GROUP_ERROR}",
            f"arn:aws:logs:{self.aws_region}:{self.aws_account_id}:log-group:{GLUE_LOG_GROUP_OUTPUT}",
        ]

    def _fix_newlines(self, message):
        return message.replace('\\n', '\n').replace('\\t', '\t')

    def _jsonify_message(self, message):
        import re

        # Look for JSON pattern starting with { or [ after the timestamp and severity
        #json_match = re.search(r'.*?({[\s\S]*}|\[[\s\S]*\])$', message) # any JSON lines
        json_match = re.search(r'^(.*\b(ERROR|WARN|EXCEPTION)\b.*?)({[\s\S]*}|\[[\s\S]*\])$', message) # only error-type JSON lines

        if json_match:
            try:
                # Get the JSON part
                json_str = json_match.group(1)
                parsed_json = json.loads(json_str)

                # Print the prefix (non-JSON part)
                prefix = message[:message.index(json_str)]

                return prefix + "\n" + self._fix_newlines(json.dumps(parsed_json, indent=2))
            except json.JSONDecodeError:
                return self._fix_newlines(message)  # Fall back to original if JSON parsing fails
        else:
            return self._fix_newlines(message)  # No JSON found, print original

    def _pretty_print_log_event(self, log_event):
        # First check if this is a task log
        if "_g-" in log_event.get('logStreamName', ''):
            return  # Skip task logs

        error_log_group_identifier = f"{self.aws_account_id}:{GLUE_LOG_GROUP_ERROR}"
        output_log_group_identifier = f"{self.aws_account_id}:{GLUE_LOG_GROUP_OUTPUT}"

        log_timestamp = log_event['timestamp']
        log_message = log_event['message']
        log_group = log_event['logGroupIdentifier']

        # Ignore empty and known log noise
        if not log_message:
            return # Skip empty log messages
        elif log_group == error_log_group_identifier:
            return # Only watch error log for special substrings indicating the run should terminate early, not to print to the user. Review these in CloudWatch for debugging.
        elif any(key in log_message for key in utils.LOG_PATTERN_IGNORE_LIST):
            return # Skip known noisy log patterns

        if log_group == output_log_group_identifier:
            # No formatting allowed
            # Cuz `find` gets item contents split across separate messages so we need to be 100% pass-thru
            formatted_message = self._jsonify_message(log_message)
        else:
            # Non-output logs can decorate with what special non-output place they came from
            formatted_message = f'[{log_group}] {self._jsonify_message(log_message)}'

        # Pretty print useful info w/ console coloring for easier readability
        # Should this stuff not be using log.error() and so on?
        # end='' prevents newlines which is important since messages can be in multiple events
        # Really we should be buffering til we hit a newline
        if any(key in log_message.lower() for key in utils.CONFIG_LOG_MESSAGE_KEYS):
            print(ColorCodes.GRAY + formatted_message + ColorCodes.RESET, end='')
        elif any(key in log_message.lower() for key in utils.STD_ERROR_MESSAGE_KEYS):
            print(ColorCodes.PINK + formatted_message + ColorCodes.RESET, file=sys.stderr, end='')
        elif any(key in log_message.lower() for key in utils.WARN_LOG_MESSAGE_KEYS):
            print(ColorCodes.YELLOW + formatted_message + ColorCodes.RESET, end='')

        else:
            print(formatted_message, end='') # not our job to add newlines

    def _is_job_state_unhealthy(self, log_event):
        """
        Review all Log Groups for any log events that indicate the job is in an unhealthy state.

        WARNING: This may not work as expected if certain log groups are disabled (ex. '/jobs/error')
                 since that may be where the useful unhealthy log events are generated.
        """
        return any(key in log_event['message'] for key in utils.UNHEALTHY_STATE_LOG_MESSAGE_KEYS)

    def _wait_for_log_groups_to_exist(self, log_group_arns):
        """
        Wait for log groups to exist. Log groups should already be created during bootstrap,
        but this provides a fallback in case they don't exist yet.
        """
        retries = 0
        while retries < LIVE_TAIL_MAX_RETRIES:
            # Extract log group names from ARNs
            input_log_groups = set(arn.split(':')[-1] for arn in log_group_arns)

            try:
                # Get all the log groups in one API call
                response = self.logs_client.describe_log_groups(logGroupNamePrefix='/aws-glue/jobs/')
                existing_log_groups = {group['logGroupName'] for group in response['logGroups']}
                # Check if all input log groups are in the existing log groups
                all_exist = input_log_groups.issubset(existing_log_groups)
                if all_exist:
                    return
            except ClientError as e:
                print(f"Error describing log groups: {e}")

            retries += 1
            log.debug(f"Log group(s) not found: {log_group_arns}. ")
            if retries < LIVE_TAIL_MAX_RETRIES:
                log.debug(f"Retry {retries}/{LIVE_TAIL_MAX_RETRIES} in {LIVE_TAIL_RETRY_WAIT_TIME_IN_SECONDS} seconds...")
                time.sleep(LIVE_TAIL_RETRY_WAIT_TIME_IN_SECONDS)
            else:
                exit(f"Log groups not found: {log_group_arns}. Max retries {retries}/{LIVE_TAIL_MAX_RETRIES} reached! Exiting.")

    def _watch_log_group(self, job_run_id, log_group_arn, job_unhealthy_event):
        """
        Watch a specific log group for a Glue job in a separate thread.
        
        Args:
            job_run_id: The ID of the Glue job run
            log_group_arn: The ARN of the log group to watch
            job_unhealthy_event: A threading.Event that will be set if the job is found to be unhealthy
        """
        log_group_name = log_group_arn.split(':')[-1]
        log.debug(f"Starting live tail for log group: {log_group_name}")
        
        # Wait for this specific log group to exist
        self._wait_for_log_groups_to_exist([log_group_arn])
        
        succeeded_counter = 0
        reassembler = GlueLogReassembler()  # Each thread gets its own reassembler
        
        try:
            # Start live tail for this specific log group
            response = self.logs_client.start_live_tail(
                logGroupIdentifiers=[log_group_arn],
                logStreamNamePrefixes=[job_run_id] 
            )
            event_stream = response['responseStream']
            
            for event in event_stream:
                # Check if we should stop because the job is in a terminal state
                job_run_state = self._get_job_run_state(job_run_id)
                if job_run_state in TERMINAL_JOB_STATES or job_unhealthy_event.is_set():
                    event_stream.close()
                    break

                # Handle when session is started
                if 'sessionStart' in event:
                    session_start_event = event['sessionStart']

                # Handle when log event is given in a session update
                elif 'sessionUpdate' in event:
                    log_events = event['sessionUpdate']['sessionResults']

                    # Add to reassembler and process ready events
                    reassembled_events = reassembler.process(log_events)

                    for log_event in reassembled_events:
                        self._pretty_print_log_event(log_event)
                        if self._is_job_state_unhealthy(log_event):
                            log.error(f"Logs from {log_group_name} indicate the Glue Job is unhealthy! Shutting down...")
                            job_unhealthy_event.set()  # Signal to other threads that the job is unhealthy
                            self._stop_glue_job(job_run_id)
                            return

                    if job_run_state == SUCCEEDED_STATE:
                        log.debug(f"Glue Job complete! Checking for any remaining logs in {log_group_name}...")
                        if not log_events:  # Update counter for closing out the job.
                            succeeded_counter += 1
                        else:  # Reset counter since more logs still coming through.
                            succeeded_counter = 0
                        if succeeded_counter > LIVE_TAIL_SUCCESS_SHUTDOWN_MAX_COUNT:
                            log.debug(f"All remaining logs from {log_group_name} appear to have been captured! Closing Live Tail session...")
                            event_stream.close()  # This will end the Live Tail session.
                            break

                else:
                    raise RuntimeError(str(event))

            # Final flush for any remaining buffered logs
            log.debug(f"Flushing remaining buffered/reassembled logs from {log_group_name}...")
            for log_event in reassembler.flush():
                self._pretty_print_log_event(log_event)

        except Exception as e:
            log.error(f"Unexpected error occurred in {log_group_name} live tail: {str(e)}.")
            return

    def _watch_glue_job(self, job_run_id):
        # Fetch log group identifiers (ARNs)
        log_group_arns = self._get_log_group_arns()
        
        # Create a shared event that will be set if any thread detects the job is unhealthy
        job_unhealthy_event = threading.Event()
        
        # Create and start a thread for each log group
        log_threads = []
        for log_group_arn in log_group_arns:
            thread = threading.Thread(
                target=self._watch_log_group,
                args=(job_run_id, log_group_arn, job_unhealthy_event),
                daemon=True
            )
            log_threads.append(thread)
            thread.start()
            
        # Wait for all threads to complete
        for thread in log_threads:
            thread.join()

    def _get_job_run_state(self, job_run_id):
        try:
            response = self.glue_client.get_job_run(
                JobName=GLUE_JOB_NAME,
                RunId=job_run_id
            )
            job_run_state = response['JobRun']['JobRunState']
            return job_run_state
        except Exception as e:
            log.error('Error getting job run state!', e)
            exit(f"Error getting job run state! {e}")

    def _get_job_run_error_message(self, job_run_id):
        try:
            response = self.glue_client.get_job_run(
                JobName=GLUE_JOB_NAME,
                RunId=job_run_id
            )
            return response['JobRun'].get('ErrorMessage', None)
        except Exception as e:
            log.error('Error getting job run ErrorMessage!', e)
            exit(f"Error getting job run ErrorMessage {e}")

    def _get_job_run_dpu(self, job_run_id, args):
        try:
            waitForDPU = args.get("XWaitForDPU")
            if waitForDPU:
                log.info("Waiting 40 seconds for DPU metrics to gather...")
                time.sleep(40)
            response = self.glue_client.get_job_run(
                JobName=GLUE_JOB_NAME,
                RunId=job_run_id
            )
            return response['JobRun'].get('DPUSeconds', 0)

        except Exception as e:
            log.error('Error getting job DPU seconds!', e)
            return -1

    def _get_glue_job_arguments(self, args, script_args):
        arguments = {}
        if args.get('XDebug'):
            arguments['--XDebug'] = str(True)

        # Add Unknown Arguments
        for i in range(0, len(script_args), 2):
            if script_args[i].startswith('--'):
                value = script_args[i+1] if i+1 < len(script_args) else None
                if value is not None:
                    if type(value) == bool:
                        value = str(value)
                    key = script_args[i][2:]
                    if key == "verb": # Server-side thinks of verb as XAction
                        key = "XAction"
                    arguments[f"--{key}"] = value

        log.debug(f"All Glue Job args: {arguments}")
        return arguments

    def _assert_expected_script_args(self, args, glue_job_arguments):
        """
        Confirm that known arguments passed to a script for a run align within the environment.

        This function checks if the arguments provided to a Glue job script are present
        in the current environment. It helps ensure that all necessary parameters are 
        available before the script execution proceeds.

        Args:
            args (dict): A dictionary containing the Bulk Executor arguments.
            glue_job_arguments (dict): A dictionary containing the arguments passed to the Glue job script.

        Raises:
            ValueError: If an expected argument is not found in the environment.

        Example:
            >>> args = {'XVersion': '0.0.0'}
            >>> job_args = {'--table': 'dynamo_db_table_name'}
            >>> _assert_expected_script_args(args, job_args)
        """

        # Assert Bulk Executor Glue Job Exists
        if not is_existing_glue_job(self.glue_client):
            raise AssertionError("Bulk Executor Glue Job does not exist!")

        # Assert remote and local versions aligned
        assert_version_parity(self.glue_client, args)

    def _start_glue_job(self, glue_job_arguments, args):
        try:
            response = self.glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments=glue_job_arguments,
                ExecutionClass=args.get('XExecutionClass', GlueJobDefaults.ExecutionClass.value),
                NumberOfWorkers=args.get('XNumberOfWorkers', GlueJobDefaults.NumberOfWorkers.value),
                Timeout=args.get('XTimeout', GlueJobDefaults.Timeout.value),
                WorkerType=args.get('XWorkerType', GlueJobDefaults.WorkerType.value),
            )
            return response['JobRunId']
        except Exception as e:
            log.error('Error starting Bulk Executor Glue Job!')
            error_code = None
            if hasattr(e, 'response') and e.response:
                error_response = e.response.get('Error')
                if error_response:
                    error_code = error_response.get('Code')
            if error_code == 'ExpiredTokenException':
                exit(f"Auth Credentials failed with an ExpiredTokenException! {e}")
            if error_code == 'EntityNotFoundException':
                exit(f"Could not find the Glue job 'bulk_dynamodb' in account '{self.aws_account_id}' in region '{self.aws_region}', perhaps you need to run bootstrap...")
            else:
                exit(f"Unhandled Exception! {e}")
            exit(e) # could be smarter?

    def _stop_glue_job(self, job_run_id):
        try:
            response = self.glue_client.batch_stop_job_run(
                JobName=GLUE_JOB_NAME,
                JobRunIds=[job_run_id]
            )
            if 'SuccessfulSubmissions' in response:
                log.info(f"Stopping Bulk Executor Glue Job {job_run_id}!")
            elif 'Errors' in response:
                log.error(response['Errors'])
            return
        except Exception as e:
            log.error(f"Error stopping job: {e}")

    def _watch_for_interrupt(self, job_run_id):
        job_run_state = self._get_job_run_state(job_run_id)
        with GracefulInterruptHandler() as h:
            while job_run_state not in TERMINAL_JOB_STATES and job_run_state != SUCCEEDED_STATE:
                if h.interrupted:
                    print("\n") # Line break to prevent CLI clutter
                    log.info(f"You pressed ^C! Stopping Glue job...")
                    self._stop_glue_job(job_run_id)
                    return # Prevent a continuous loop until the terminal job state persists.
                time.sleep(1)
                job_run_state = self._get_job_run_state(job_run_id)

    def run(self, args, script_args):
        log.debug(f"XArgs: {args}")
        log.debug(f"Script args: {script_args}")

        try:
            glue_job_arguments = self._get_glue_job_arguments(args, script_args)
            self._assert_expected_script_args(args, glue_job_arguments)
        except BaseException as e: # Root catch intentional.
            error_message = str(e)
            log.error(error_message)
            log.info("Job not executed.")
            return

        log.info("""

The bulk executor job cost consists of DynamoDB and Glue costs
For small jobs, the Glue cost portion is usually dominating
Using fewer Glue workers for small jobs, through the --XNumberOfWorkers parameter, will often reduce the Glue costs
For large jobs, where the cost is more significant, the DynamoDB cost portion is usually dominating
The DynamoDB cost will be estimated below
The Glue cost estimation isn't provided since it is based on DPU hours being used by the job, which is hard to estimate in advance
You can run the script with the --XWaitForDPU parameter in order to print the used Glue DPU hours at the end of the job
""")

        log.info("Starting Bulk Executor Glue Job...")

        # Start the Glue job
        job_run_id = self._start_glue_job(glue_job_arguments, args)
        job_start_time = datetime.now()

        log.info(f"Bulk Executor Glue Job started with job run ID: {job_run_id}")
        log.info(f"Job start time: {job_start_time}")

        # Watch on a separate thread to ensure Job Interruption (^C) exits quickly.
        watch_glue_job_thread = threading.Thread(target=self._watch_glue_job, args=(job_run_id,), daemon=True)
        watch_glue_job_thread.start()

        log.info('Press Ctrl+C to cancel Bulk Executor Glue Job.')

        self._watch_for_interrupt(job_run_id) # 'signal' only works in main thread of the main interpreter

        # Job is shutting down
        job_run_state = self._get_job_run_state(job_run_id)
        job_run_error_message = self._get_job_run_error_message(job_run_id)

        job_end_message = None
        if job_run_state == STOPPING_STATE:
            job_end_message = "Job is stopping."
        elif job_run_state == STOPPED_STATE:
            job_end_message = "Job was stopped."
        elif job_run_state == FAILED_STATE:
            job_end_message = "Job failed."
        elif job_run_state == TIMEOUT_STATE:
            job_end_message = "Job timed out."
        elif job_run_state == SUCCEEDED_STATE:
            job_end_message = "Job completed successfully."
        else:
            log.error(f"Unhandled Job State: {job_run_state}")

        job_end_time = datetime.now()
        job_duration = job_end_time - job_start_time

        dpu_seconds = self._get_job_run_dpu(job_run_id, args)
        dpu_hours = dpu_seconds / 3600

        # Usually this is 0.0 unless we've waited for DPUs to arrive
        if dpu_hours > 0.0:
            log.info(f"{job_end_message} Job duration: {str(job_duration).split('.')[0]} ({dpu_hours:.2f} DPU hours)")
        else:
            log.info(f"{job_end_message} Job duration: {str(job_duration).split('.')[0]}")

        if job_run_error_message:
            log.error(job_run_error_message)
