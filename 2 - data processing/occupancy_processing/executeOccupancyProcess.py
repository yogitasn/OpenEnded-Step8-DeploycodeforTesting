import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType,DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql.functions import col,array_contains,date_format,regexp_replace
import logging
import configparser
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
import re
import os
import re
import time

from occupancy_processing import miscProcess

SCRIPT_NAME= os.path.basename(__file__)

def global_SQLContext(spark1):
    global spark
    spark= spark1

def global_EffectDt(iEffevtDt):
    global EffectvDt
    EffectvDt=datetime.strptime(iEffevtDt, "%Y-%m%d").date()

def get_currentDate():
    current_time = datetime.now()
    str_current_time = current_time.strftime("%Y-%m-%d")
    miscProcess.log_print(str_current_time)

def regex_replace_values():
    regexp_replace

def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")

def remove__parenthesis(col):
    return F.regexp_replace(col, "\(|\)", "")

def date_format(col, formattype):
    return F.date_format(col, formattype)

def timestamp_format(col, timestampformat):
    return F.to_timestamp(col, format=timestampformat)
    
# Read Parquet function - input source config, partition name and column mapping list
def sourceOccupancyReadParquet(occupancyFilePath, custom_schema, partition_value):

    miscProcess.log_info(SCRIPT_NAME, "Reading Occupancy CSV file...")
    print("Reading Occupancy CSV file")

    source_data_info = {}
    source_data_info["type"] = "CSV"

    #filepath = source_config['sources']['driverSource']["filePath"]
    print("Occupancy file path : {}".format(occupancyFilePath))


    try:
        occupancy = spark.read.format("csv") \
                    .option("header", True) \
                    .schema(custom_schema) \
                    .load(occupancyFilePath)


 #       occupancy = occupancy.withColumn("_PARTITION_VALUE_",lit(partition_value))

    except Exception as e:
        miscProcess.log_info(SCRIPT_NAME, "error in reading csv: {}".format(e))
    

    source_data_info["occupancyFilePath"] = occupancyFilePath
    source_data_info["partition"] = str(partition_value)

    occupancy.show(5)

    return (occupancy, source_data_info)

def createStationIDDF():

    occ_df_2020 = spark.read.format("csv") \
                        .option("header", True) \
                        .schema(schema) \
                        .load("C://Test//Paid_Parking.csv")


    occ_df_2020 = occ_df_2020.withColumn('Station_Id',remove_non_word_characters(F.col('Station_Id')))

    occ_df_2020 = occ_df_2020.withColumn('Longitude',F.split('Location',' ').getItem(1)) \
                    .withColumn('Latitude',F.split('Location',' ').getItem(2))


    occ_df_2020 = occ_df_2020.withColumn('Latitude',remove__parenthesis(F.col("Latitude"))) \
                    .withColumn('Longitude',remove__parenthesis(F.col("Longitude")))


    occ_df_2020 = occ_df_2020.withColumn("Latitude",occ_df_2020["Latitude"].cast(DoubleType())) \
                    .withColumn("Longitude",occ_df_2020["Longitude"].cast(DoubleType()))


    occ_df_2020 = occ_df_2020.drop('Location')


    # get the distinct Station_Id, Longitude and Latitude
    station_id_lookup = occ_df_2020.select('Station_Id','Longitude','Latitude').distinct()

    station_id_lookup.persist()

    # Broadcast the smaller dataframe as it contains few 1000 rows
    F.broadcast(station_id_lookup)

    return station_id_lookup




def executeOccupancyOperations(src_df, output, cols_list, partn_col, max_retry_count,retry_delay):

    PartitionColumn = partn_col

    ReturnCode=0
    rec_cnt=0
    RetryCt =0
    Success=False

    while(RetryCt < max_retry_count) and not Success:
        
        try:
            Success = True
            # reading from DBFS
            input_df = src_df
        
        except:
            Success = False
            RetryCt +=1
            if RetryCt == max_retry_count:
                miscProcess.log_info(SCRIPT_NAME, "Failed on reading input file after {} tries: {}".format(max_retry_count))
                ReturnCode=1
                return ReturnCode, rec_cnt

            else:
                miscProcess.log_info(SCRIPT_NAME, "Failed on reading input file, re-try in {} seconds ".format(retry_delay))


    select_df = input_df.select([colname for colname in input_df.columns if colname in (cols_list)])

    for column in cols_list:
        if column == 'Station_Id':
        
            select_df = select_df.withColumn(column,remove_non_word_characters(F.col("Station_Id")))
            select_df = select_df.withColumn(col,select_df[col].cast(IntegerType()))
            
        elif column == 'OccupancyDateTime':
            spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
            select_df = select_df.withColumn(column, timestamp_format(F.col(column), "mm/dd/yyyy hh:mm:ss a"))
             
            date_dim = select_df.withColumn('day_of_week',date_format(F.col(column), "EEEE")) \
                                .withColumn('month',date_format(F.col(column), "MMMM")) 
                
            date_dim=date_dim.select('OccupancyDateTime','day_of_week','month')

            select_df = select_df.withColumn(PartitionColumn ,date_format(F.col(column), "MMMM"))

        elif column == 'Location':
            split_col = ['Longitude','Latitude']
            
            select_df=select_df.withColumn(split_col[0],F.split(column,' ').getItem(1)) \
                        .withColumn(split_col[1],F.split(column,' ').getItem(2))


            select_df=select_df.withColumn(split_col[0],remove__parenthesis(col(split_col[0]))) \
                               .withColumn(split_col[1],remove__parenthesis(col(split_col[1]))) 
                   


            select_df = select_df.withColumn(split_col[0],select_df[split_col[0]].cast(DoubleType())) \
                                 .withColumn(split_col[1],select_df[split_col[1]].cast(DoubleType())) 

            select_df=select_df.drop(column)
            
     #   select_df = select_df.select(cols_list)
        #select_df = select_df.select([colname for colname in input_df.columns if colname in (cols_list)])

        RetryCt = 0
        Success = False

        while(RetryCt < max_retry_count) and not Success:
            try:
                Success = True
                miscProcess.log_print("Writing occupancy dataframe to output file: {}".format(output))
                select_df.write.mode("overwrite").partitionBy(PartitionColumn).parquet("C://Output//Parking.parquet")

                miscProcess.log_print("Writing date dimension to output file: {}".format(output))
                date_dim.write.mode("overwrite").partitionBy(PartitionColumn).parquet("C://Output//date_dim.parquet")
            except:
                Sucess = False
                RetryCt += 1
                if RetryCt == max_retry_count:
                    miscProcess.log_info(SCRIPT_NAME, "Failed on writing to Output after {} tries: {} ".format(max_retry_count, output))
                    ReturnCode = 2
                    return ReturnCode, rec_cnt
                else:
                    miscProcess.log_info(SCRIPT_NAME, "Failed on writing to Output, re-try in {} seconds ".format(retry_delay))
                    time.sleep(retry_delay)

        miscProcess.log_print("Number of Records Processed: {}".format(rec_cnt))
        return ReturnCode, rec_cnt


def executeHistoricOccupancyOperations(output, cols_list, partn_col, max_retry_count,retry_delay):
    
    PartitionColumn = partn_col
    station_id_lookup = createStationIDDF()

    occ_df = occ_df\
                .join(station_id_lookup, ['Station_Id'], how='left_outer')\
                .select(occ_df.OccupancyDateTime,occ_df.Station_Id,\
                        occ_df.Occupied_Spots,occ_df.Available_Spots,\
                        station_id_lookup.Longitude,station_id_lookup.Latitude)

    while(RetryCt < max_retry_count) and not Success:
        try:
            Success = True
            occ_df.write.mode("overwrite").partitionBy(PartitionColumn).parquet(output)
        except:
            Sucess = False
            RetryCt += 1
            if RetryCt == max_retry_count:
                miscProcess.log_info(SCRIPT_NAME, "Failed on writing to Output after {} tries: {} ".format(max_retry_count, output))
                ReturnCode = 2
                return ReturnCode, rec_cnt
            else:
                miscProcess.log_info(SCRIPT_NAME, "Failed on writing to Output, re-try in {} seconds ".format(retry_delay))
                time.sleep(retry_delay)

    miscProcess.log_print("Number of Records Processed: {}".format(rec_cnt))
    return ReturnCode, rec_cnt


def main():

#    logger = spark._jvm.org.apache.log4j
 #   logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  #  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    executeOccupancyOperations(src_df, output, cols_list, cols_dict, partn_col, max_retry_count,retry_delay)


if __name__=='__main__':
    log_file = 'test.log'
    miscProcess.initial_log_file(logfile)
    main()


            






    



