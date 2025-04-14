# Library Imports
import os, time
from datetime import datetime
from helpers import *
from pyspark.dbutils import DBUtils

# Import dbutils
dbutils = DBUtils()


class CustomLoggerTimer:
    def __init__(self, spark, log_file_name: str, refresh_data: bool):
        """
        Initializes the CustomLoggerTimer instance.

        Input:
        - log_file_name (str): The name of the log file.

        Output:
        - Initializes the log file path by calling setup_logging().
        """
        self.refresh_data = refresh_data
        self.log_file_name = log_file_name
        self.log_file_write_path = self.setup_logging()
        self.spark = spark

    def setup_logging(self):
        """
        Sets up the logging environment, creating the log file and ensuring
        the parent directory exists.

        Inputs:
        - None (uses instance variable `log_file_name`).

        Outputs:
        - Returns (str): The path to the created log file.
        """
        # Correct file path for writing in DBFS
        if is_running_in_databricks():
            file_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
        else: # Get the current script's directory
            file_dir = f"./{os.path.dirname(os.getcwd())}"
        log_file_path = f"{file_dir}/{self.log_file_name}"
        # Ensure the parent directory exists
        dir_path = os.path.dirname(log_file_path)
        os.makedirs(dir_path, exist_ok=True)
        # Remove the log file if it exists
        if os.path.isfile(log_file_path) and self.refresh_data:
            os.remove(log_file_path)
            print(f"Existing log file removed: {log_file_path}")
        # Create an empty log file
        with open(log_file_path, 'a') as log_file:
            log_file.write("")
        print(f"log_file_path: {log_file_path}")
        return log_file_path

    def info(self, msg: str):
        """
        Writes a log message to the log file.
        
        Input:
        - msg (str): The message to write to the log file.

        Output:
        - None (writes the message to the log file).
        """
        with open(self.log_file_write_path, 'a') as log_file:
            log_file.writelines(msg + '\n')

    def read_logging(self):
        """
        Reads the content of the log file and prints it to the console.

        Input:
        - None (uses instance variable `log_file_write_path`).

        Output:
        - Prints the content of the log file if it exists.
        """
        log_file_path = self.log_file_write_path
        # Check if the file exists before attempting to read it
        if os.path.exists(log_file_path):
            with open(log_file_path, 'r') as log_file:
                log_content = log_file.read()
            return log_content
        else:
            print(f"Log file not found at {log_file_path}")

    def timer(self, func):
        """
        A decorator that logs the execution time and timestamps of a function.

        Input:
        - func (callable): The function to be decorated.

        Output:
        - wrapper (callable): The decorated function with added logging.
        """
        def wrapper(*args, **kwargs):
            start_time = time.time()
            start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Get current date and time
            result = func(*args, **kwargs)
            end_time = time.time()
            end_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Get end date and time
            self.info(
                f"{start_dt}: Function={func.__name__!r}; started={start_dt}; "
                f"ended={end_dt}, total_executed_time={end_time - start_time:.4f}s; "
                f"os_pid={str(os.getpid())}; hostname={str(os.uname().nodename)}; hostname_ip={str(get_external_ip())} "
                f"total_running_executors={str(get_total_executors(self.spark))};"
            )
            return result
        return wrapper


# # Initialize logger and timing functions
# logger = CustomLoggerTimer("extract_images.log")