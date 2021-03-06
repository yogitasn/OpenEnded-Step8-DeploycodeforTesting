import os, sys, json
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType,DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from occupancy_processing import miscProcess

SCRIPT_NAME = os.path.basename(__file__)

def global_parameters(temp_path):
    global g_temp_path
    g_temp_path = temp_path

""" Function to read the json configuration file """
def json_reader(json_file):
    with open(json_file) as jfile:
        df_dict = json.load(jfile)
    return df_dict


""" Function to get the dataframe long name """
def dataframe_long_na(df_dict):
    return(df_dict['dataframeName'])


""" Function to get the dataframe load strategy"""
def get_dataframe_loadStrategy(df_dict):
    dataframe_loadStatergy = df_dict['targetDataframeDetails']['DFLoadStrategy'].upper()
    miscProcess.log_info(SCRIPT_NAME, "Dataframe loadStatergy: {}".format(dataframe_loadStatergy))
    return table_loadStatergy


""" Function to get the dataframe load frequency """

def load_freq(df_dict):
    load_freq = df_dict['targetDataframeDetails']['dataframeFrequency']
    return load_freq


""" Function to get the dataframe partition column """
def partition_column(df_dict):

    part_col_lcase = df_dict['targetDataframeDetails']['dataframePartition']

    miscProcess.log_info(SCRIPT_NAME, "Partition Column : {}". format(part_col_lcase))

    return part_col_lcase

""" Function to count the number of columns """

def count_columns(df_dict):
    count = 0
    for x in df_dict:
        if isinstance(df_dict[x], list):
            count += len(df_dict[x])

    print(count)
    return count

""" Function to build the dataframe column list from the json file"""
def build_dataframe_column_list(df_dict):

    column_list = []

    column_count = len(df_dict['targetDataframeDetails']['dataframeColumnInfo'])

    for i in range(0, column_count):
        column_list.append(df_dict['targetDataframeDetails']['dataframeColumnInfo'][i]['columnName'].lower())


    miscProcess.log_info(SCRIPT_NAME, "Dataframe Column List: {}".format(column_list))
    return column_list

""" Function to build the dataframe schema using the input from json file"""
def get_dataframe_schema(df_dict):

    dtypes = {
        "IntegerType()": IntegerType(),
        "StringType()": StringType(),
        "DoubleType()": DoubleType()
    }

    cust_schema = StructType()

    column_count = len(df_dict['sources']['driverSource']['fields'])

    print(column_count)

    for i in range(0, column_count):
        cust_schema.add(df_dict['sources']['driverSource']['fields'][i]['name'],\
             dtypes[df_dict['sources']['driverSource']['fields'][i]['type']], True)

    return cust_schema


""" Function to get the dataframe input path """
def get_source_driverFilerPath(df_dict):
    driverFilePath = df_dict['sources']['driverSource']["filePath"]
    miscProcess.log_info(SCRIPT_NAME, "driverFilePath: {}".format(driverFilePath))
    return driverFilePath

""" Function to get the dataframe output path """
def get_source_OutputPath(df_dict):
    outputFilePath = df_dict['sources']['driverSource']["OutputPath"]
    miscProcess.log_info(SCRIPT_NAME, "OutputPathFilePath: {}".format(outputFilePath))
    return outputFilePath

""" Function to get the dimension output path """
def get_source_dateDimOutputPath(df_dict):
    datedimOutputPath = df_dict['sources']['driverSource']["DimOutputPath"]
    miscProcess.log_info(SCRIPT_NAME, "Date Dim OutputPathFilePath: {}".format(datedimOutputPath))
    return datedimOutputPath


def main(input_file):
    print(input_file)
    df_dict = json_reader(input_file)
    print(df_dict)
    print(load_freq(df_dict))

if __name__ == '__main__':
    main(sys.argv[1])


  