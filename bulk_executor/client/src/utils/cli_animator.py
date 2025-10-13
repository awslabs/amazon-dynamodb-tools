import itertools
import time

# Spinner characters
SPINNER_CHARS = ['|', '/', '-', '\\']
NUM_SPINNERS = 4  # Number of spinner characters to use
SPINNER_INTERVAL_SECONDS = 0.15  # Adjusted interval for a smoother animation

def with_spinner_animation(wait_time_in_seconds, animation_message=""):
    start_time = time.time()
    while time.time() - start_time < wait_time_in_seconds:
        for char in itertools.cycle(SPINNER_CHARS):
            elapsed_time = time.time() - start_time
            if elapsed_time >= wait_time_in_seconds:
                break

            # Create spinner animation with progress message
            spinner_str = f"{char} {animation_message}"

            # Print the spinner animation
            print(f"\r{spinner_str}", end="", flush=True)

            time.sleep(SPINNER_INTERVAL_SECONDS)
