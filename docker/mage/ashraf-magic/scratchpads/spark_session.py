"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""

from mage_ai.data_preparation.repo_manager import RepoConfig, get_repo_path
from mage_ai.services.spark.config import SparkConfig
from mage_ai.services.spark.spark import get_spark_session

repo_config = RepoConfig(repo_path=get_repo_path())
spark_config = SparkConfig.load(
    config=repo_config.spark_config)

spark = get_spark_session(spark_config)
print(spark.sql('select 1'))

