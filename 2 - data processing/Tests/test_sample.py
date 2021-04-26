import findspark

findspark.init()

findspark.find()

import pyspark
import logging
import sys
from operator import add
import pytest
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from pyspark.sql.types import ArrayType, DoubleType, BooleanType, StringType
from chispa.column_comparer import assert_column_equality
from pathlib import Path
import json
import os
SCRIPT_NAME= os.path.basename(__file__)

path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
logging.info(path)

try:
    sys.path.insert(1, path+"/2 - data processing/occupancy_processing") # the type of path is string
    # /databricks/python/lib/python3.7/site-packages/Pipelineorchestration/

    from occupancy_processing import executeBlockface
    from occupancy_processing import executeOccupancyProcess
    from occupancy_processing import processDataframeConfig
    from occupancy_processing import miscProcess

except (ModuleNotFoundError, ImportError) as e:
    print("{} fileure".format(type(e)))
else:
    print("Import succeeded")

def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = (SparkSession
             .builder
             .master('local[2]')
             .appName('pytest-pyspark-local-testing')
             .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark

pytestmark = pytest.mark.usefixtures("spark_session")
def test_remove_non_word_characters(spark_session):
    data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("77,990", "77990"),
        (None, None)
    ]
    df = spark_session.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", executeOccupancyProcess.remove_non_word_characters(F.col("name")))
    assert_column_equality(df, "clean_name", "expected_name")


pytestmark = pytest.mark.usefixtures("spark_session")
def test_column_list(spark_session):
    miscProcess.global_SQLContext(spark_session)
    miscProcess.initial_log_file('test_log.log')

    json_file = 'test1.json'

    with open(json_file) as jfile:
        df_dict = json.load(jfile)
    
    actual_list=processDataframeConfig.build_dataframe_column_list(df_dict)

    expected_list= ['occupancydatetime','occupied_spots']
    print(actual_list)

    assert actual_list[0] == expected_list[0]
    assert actual_list[1] == expected_list[1]


pytestmark = pytest.mark.usefixtures("spark_session")
def test_file_path(spark_session):
    
    miscProcess.global_SQLContext(spark_session)
    miscProcess.initial_log_file('test_log.log')

    json_file = 'test1.json'

    with open(json_file) as jfile:
        df_dict = json.load(jfile)
    
    actual_filepath=processDataframeConfig.get_source_driverFilerPath(df_dict)

    expected_filePath= "C:\\Test\\Paid_Parking.csv"

    assert actual_filepath == expected_filePath


pytestmark = pytest.mark.usefixtures("spark_session")
def test_dataframe_partition(spark_session):
    
    miscProcess.global_SQLContext(spark_session)
    miscProcess.initial_log_file('test_log.log')

    json_file = 'test1.json'

    with open(json_file) as jfile:
        df_dict = json.load(jfile)
    
    actual_partition=processDataframeConfig.partition_column(df_dict)

    expected_partition= "MONTH"

    assert actual_partition == expected_partition


pytestmark = pytest.mark.usefixtures("spark_session")
def test_remove_parenthesis_characters(spark_session):
    data = [
        ("(123.88", "123.88"),
        ("6788.9)", "6788.9")
    ]
    df = spark_session.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", executeOccupancyProcess.remove__parenthesis((F.col("name"))))
    
    actual_data_list =df.select('clean_name').collect()

    actual_data_array = [float(row['clean_name']) for row in actual_data_list]

    expected_data_list = df.select('expected_name').collect()

    expected_data_array = [float(row['expected_name']) for row in expected_data_list]

    assert actual_data_array[0] == expected_data_array[0]
    assert actual_data_array[1] == expected_data_array[1] 


from pyspark.sql import functions as F
pytestmark = pytest.mark.usefixtures("spark_session")
def test_date_format(spark_session):
    spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

   
    data = [
        ("04/15/2021 02:33:00 PM","April"),
        ("05/17/2021 08:01:00 AM","May"),
    ]
    df = spark_session.createDataFrame(data, ["Datetime","expected_date_fmt_value"])

    df=df.withColumn("Datetime", executeOccupancyProcess.timestamp_format(F.col("Datetime"),"MM/dd/yyyy hh:mm:ss a"))


    df = df.withColumn("actual_month", executeOccupancyProcess.date_format(F.col("Datetime"),"MMMM"))

    actual_month_data_list =df.select('actual_month').collect()

    actual_month_data_array = [str(row['actual_month']) for row in actual_month_data_list]

    expected_month_data_list = df.select('expected_date_fmt_value').collect()

    expected_month_data_array = [str(row['expected_date_fmt_value']) for row in expected_month_data_list]

    assert actual_month_data_array[0] == expected_month_data_array[0]
    assert actual_month_data_array[1] == expected_month_data_array[1]

pytestmark = pytest.mark.usefixtures("spark_session")
def test_read_blockface(spark_session):
    spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    blockface_config_dict = processDataframeConfig.json_reader("..//common//blockface.json")
    
    blockfacefilePath = processDataframeConfig.get_source_driverFilerPath(blockface_config_dict)

    # Get Target Table Schema
    TargetDataframeSchema = processDataframeConfig.get_dataframe_schema(blockface_config_dict)

    blockface = spark_session.read.format("csv") \
            .option("header", True) \
            .schema(TargetDataframeSchema) \
            .load(blockfacefilePath)

    blockface.printSchema()

    blockface.head(4)
#    assert actual_month_data_array[0] == expected_month_data_array[0]
#    assert actual_month_data_array[1] == expected_month_data_array[1]