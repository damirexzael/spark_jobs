import os
import sys

from tests.spark_session_manager import SparkSessionManager

SparkSessionManager.start()
sys.path.append(os.path.join(os.path.dirname(__file__), '../jobs/python_libs'))
