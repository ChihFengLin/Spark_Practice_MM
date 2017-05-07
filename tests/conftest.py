""" pytest fixtures that can be resued across tests. the filename needs to be conftest.py
"""

# make sure env variables are set correctly
import os
import sys
import findspark  # this needs to be the first import
findspark.init()

import logging
import pytest

from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """

    conf = SparkConf().setMaster("local").setAppName("pytest-pyspark-local-testing")
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())
    quiet_py4j()

    return sc


@pytest.fixture(scope="session")
def sql_context(spark_context):
    """  fixture for creating a SQL Context. Creating a fixture enables it to be reused across all
        tests in a session
    Args:
        spark_context: spark_context fixture
    Returns:
        SQLContext for tests
    """
    return SQLContext(spark_context)


@pytest.fixture(scope="session")
def streaming_context(spark_context):
    return StreamingContext(spark_context, 1)
