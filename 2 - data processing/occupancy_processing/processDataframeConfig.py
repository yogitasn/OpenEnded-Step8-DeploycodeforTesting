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


def json_reader(json_file):
    with open(json_file) as jfile:
        df_dict = json.load(jfile)
    return df_dict

def dataframe_long_na(df_dict):
    return(df_dict['dataframeName'])

def get_table_loadStatergy(df_dict):
    dataframe_loadStatergy = df_dict['targetDataframeDetails']['DFLoadStrategy'].upper()
    miscProcess.log_info(SCRIPT_NAME, "Dataframe loadStatergy: {}".format(dataframe_loadStatergy))
    return table_loadStatergy

def load_freq(df_dict):
    load_freq = df_dict['targetDataframeDetails']['dataframeFrequency']
    return load_freq

def partition_column(df_dict):
   # if len(df_dict['targetDataframeDetails']['dataframePartition']) > 1:
    #    part_col, part_type = df_dict['targetDataframeDetails']['dataframePartition'].split(' ',1)
    #    part_col_lcase = part_col.lower()
    #else:
        #part_col_lcase = 'load_dt'

    part_col_lcase = df_dict['targetDataframeDetails']['dataframePartition']

    miscProcess.log_info(SCRIPT_NAME, "Partition Column : {}". format(part_col_lcase))

    return part_col_lcase


def count_columns(df_dict):
    count = 0
    for x in df_dict:
        if isinstance(df_dict[x], list):
            count += len(df_dict[x])

    print(count)
    return count

def build_dataframe_column_list(df_dict):

    column_list = []

    column_count = len(df_dict['targetDataframeDetails']['dataframeColumnInfo'])

    for i in range(0, column_count):
        column_list.append(df_dict['targetDataframeDetails']['dataframeColumnInfo'][i]['columnName'].lower())


    miscProcess.log_info(SCRIPT_NAME, "Dataframe Column List: {}".format(column_list))
    return column_list

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



def get_source_driverFilerPath(df_dict):
    driverFilePath = df_dict['sources']['driverSource']["filePath"]
    miscProcess.log_info(SCRIPT_NAME, "driverFilePath: {}".format(driverFilePath))
    return driverFilePath

def get_source_OutputPath(df_dict):
    outputFilePath = df_dict['sources']['driverSource']["OutputPath"]
    miscProcess.log_info(SCRIPT_NAME, "OutputPathFilePath: {}".format(outputFilePath))
    return outputFilePath


def main(input_file):
    print(input_file)
    df_dict = json_reader(input_file)
    print(df_dict)
    print(load_freq(df_dict))

if __name__ == '__main__':
    main(sys.argv[1])


  