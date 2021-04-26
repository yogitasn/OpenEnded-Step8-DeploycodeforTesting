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
import occupancy_udf



logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('..\config.cfg')

class ParkingOccupancyLoadTransform:
    """
    This class performs transformation operations on the dataset.
    Transform timestamp format, clean text part, remove extra spaces etc
    """        
    def __init__(self,spark):
        self.spark=spark
        self._load_path = config.get('BUCKET', 'WORKING_ZONE')
        self._save_path = config.get('BUCKET', 'PROCESSED_ZONE')
        

    # Tranforms the datasets from 2018 to 2020 as the data has same pattern
    def transform_load_parking_hist_2018_2020_occupancy(self, yearList):
        logging.debug("Inside transform parking occupancy dataset module")
        
        schema = StructType() \
                .add("OccupancyDateTime",StringType(),True) \
                .add("Occupied_Spots",IntegerType(),True) \
                .add("BlockfaceName",StringType(),True) \
                .add("SideOfStreet",StringType(),True) \
                .add("Station_Id",StringType(),True) \
                .add("ParkingTimeLimitCategory",IntegerType(),True) \
                .add("Available_Spots",IntegerType(),True) \
                .add("PaidParkingArea",StringType(),True) \
                .add("PaidParkingSubArea",StringType(),True) \
                .add("PaidParkingRate",DoubleType(),True) \
                .add("ParkingCategory",StringType(),True) \
                .add("Location",StringType(),True)

       

        #occ_df_2018_2020 = self.spark.read.format("csv") \
         #                       .option("header", True) \
          #                      .schema(schema) \
           #                     .load([self._load_path+"2018_Paid_Parking.csv",\
            #                            self._load_path+"2019_Paid_Parking.csv",\
             #                           self._load_path+"2020_Paid_Parking.csv"])

        occ_df_2018_2020 = self.spark.read.format("csv") \
                               .option("header", True) \
                               .schema(schema) \
                               .load(self._load_path+"20[18-20]*_Paid_Parking.csv")


        occ_df=occ_df_2018_2020.withColumn('Station_Id',regexp_replace(col('Station_Id'),'[\'\s,]',''))

        occ_df = occ_df.withColumn("Station_Id",\
                        occ_df["Station_Id"].cast(IntegerType()))

        
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        occ_df = occ_df.withColumn("OccupancyDateTime", \
                       F.to_timestamp(occ_df.OccupancyDateTime, format="mm/dd/yyyy hh:mm:ss a"))
        
        occ_df = occ_df.drop("BlockfaceName",
                   "SideOfStreet",
                   "ParkingTimeLimitCategory",
                   "PaidParkingArea",
                   "PaidParkingSubArea",
                   "PaidParkingRate",
                   "ParkingCategory")
 

        occ_df=occ_df.withColumn('Longitude',F.split('Location',' ').getItem(1)) \
                     .withColumn('Latitude',F.split('Location',' ').getItem(2))
        

        occ_df=occ_df.withColumn('Latitude',regexp_replace(col('Latitude'), "\(|\)", "")) \
                     .withColumn('Longitude',regexp_replace(col('Longitude'), "\(|\)", ""))

      
        occ_df = occ_df.withColumn("Latitude",occ_df["Latitude"].cast(DoubleType())) \
                       .withColumn("Longitude",occ_df["Longitude"].cast(DoubleType()))
        
    
        occ_df=occ_df.drop('Location')
        
        date_dim = occ_df.withColumn('day_of_week',date_format(col("OccupancyDateTime"), "EEEE")) \
                         .withColumn('month',date_format(col("OccupancyDateTime"), "MMMM"))

        date_dim=date_dim.select('OccupancyDateTime','day_of_week','month')

        occ_df.show(3,truncate=False)

        date_dim.show(3, truncate=False)


    # Tranforms the datasets from 2012 to 2017 as the column data has same pattern
    def transform_load_parking_hist_2012_2017_occupancy(self):
        logging.debug("Inside transform parking occupancy dataset module")
        
        schema = StructType() \
                .add("OccupancyDateTime",StringType(),True) \
                .add("Occupied_Spots",IntegerType(),True) \
                .add("BlockfaceName",StringType(),True) \
                .add("SideOfStreet",StringType(),True) \
                .add("Station_Id",StringType(),True) \
                .add("ParkingTimeLimitCategory",IntegerType(),True) \
                .add("Available_Spots",IntegerType(),True) \
                .add("PaidParkingArea",StringType(),True) \
                .add("PaidParkingSubArea",StringType(),True) \
                .add("PaidParkingRate",DoubleType(),True) \
                .add("ParkingCategory",StringType(),True) \
                .add("Location",StringType(),True)


        # 2012 to 2017 data doesn't have the Latitude and Longitude values similar to the latest year's data i.e. > 2018 
        # In order to propagate the Lat and Long values, lookup table is created using 2020 data and use broadcast/join to 2012_2017 data
        #occ_df_2012_2017 = self.spark.read.format("csv") \
         #       .option("header", True) \
          #      .schema(schema) \
           #     .load([self._load_path+"2012_Paid_Parking.csv",\
            #            self._load_path+"2013_Paid_Parking.csv",\
             #           self._load_path+"2014_Paid_Parking.csv",
              #          self._load_path+"2015_Paid_Parking.csv",
               #         self._load_path+"2016_Paid_Parking.csv",
                #        self._load_path+"2017_Paid_Parking.csv"])

        occ_df_2012_2017 = self.spark.read.format("csv") \
                 .option("header", True) \
                 .schema(schema) \
                 .load(self._load_path+"20[12-17]*_Paid_Parking.csv")
        
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        occ_df = occ_df_2012_2017.withColumn("OccupancyDateTime", \
                F.to_timestamp(occ_df_2012_2017.OccupancyDateTime, format="mm/dd/yyyy hh:mm:ss a"))
        
        occ_df = occ_df.drop("BlockfaceName",
                "SideOfStreet",
                "ParkingTimeLimitCategory",
                "PaidParkingArea",
                "PaidParkingSubArea",
                "PaidParkingRate",
                "ParkingCategory",
                "Location")
        
        occ_df = occ_df.withColumn("Station_Id",\
                occ_df["Station_Id"].cast(IntegerType()))


        # Creating the station lookup table using 2020 paid parking data
        occ_df_2020 = self.spark.read.format("csv") \
                .option("header", True) \
                .schema(schema) \
                .load(self._load_path+"2020_Paid_Parking.csv")

        
        occ_df_2020=occ_df_2020.withColumn('Station_Id',regexp_replace(col('Station_Id'),'[\'\s,]',''))

        occ_df_2020=occ_df_2020.withColumn('Longitude',F.split('Location',' ').getItem(1)) \
                     .withColumn('Latitude',F.split('Location',' ').getItem(2))
        

        occ_df_2020=occ_df_2020.withColumn('Latitude',regexp_replace(col('Latitude'), "\(|\)", "")) \
                     .withColumn('Longitude',regexp_replace(col('Longitude'), "\(|\)", ""))

      
        occ_df_2020 = occ_df_2020.withColumn("Latitude",occ_df_2020["Latitude"].cast(DoubleType())) \
                       .withColumn("Longitude",occ_df_2020["Longitude"].cast(DoubleType()))
        
    
        occ_df_2020=occ_df_2020.drop('Location')

        
        # get the distinct Station_Id, Longitude and Latitude
        station_id_lookup=occ_df_2020.select('Station_Id','Longitude','Latitude').distinct()

        station_id_lookup.persist()

        # Broadcast the smaller dataframe as it contains few 1000 rows
        F.broadcast(station_id_lookup)

        # Use join to propagate the Latitude and Longitude values to the data from 2012 to 2017
        occ_df_2012_2017 = occ_df_2012_2017\
                        .join(station_id_lookup, ['Station_Id'], how='left_outer')\
                        .select(occ_df_2012_2017.OccupancyDateTime,occ_df_2012_2017.Station_Id,\
                                occ_df_2012_2017.Occupied_Spots,occ_df_2012_2017.Available_Spots,\
                                station_id_lookup.Longitude,station_id_lookup.Latitude)
        
        occ_df_2012_2017.show(3,truncate=False)


    
    def transform_load_blockface_dataset(self):
        logging.debug("Inside transform blockface dataset module")
        schema = StructType() \
                .add("objectid",IntegerType(),True) \
                .add("station_id",IntegerType(),True) \
                .add("segkey",IntegerType(),True) \
                .add("unitid",IntegerType(),True) \
                .add("unitid2",IntegerType(),True) \
                .add("station_address",StringType(),True) \
                .add("side",StringType(),True) \
                .add("block_id",StringType(),True) \
                .add("block_nbr",IntegerType(),True) \
                .add("csm",StringType(),True) \
                .add("parking_category",StringType(),True) \
                .add("load",IntegerType(),True) \
                .add("zone",IntegerType(),True) \
                .add("total_zones",IntegerType(),True) \
                .add("wkd_rate1",DoubleType(),True) \
                .add("wkd_start1",IntegerType(),True) \
                .add("wkd_end1",IntegerType(),True) \
                .add("wkd_rate2",DoubleType(),True) \
                .add("wkd_start2",StringType(),True) \
                .add("wkd_end2",StringType(),True) \
                .add("wkd_rate3",DoubleType(),True) \
                .add("wkd_start3",StringType(),True) \
                .add("wkd_end3",StringType(),True) \
                .add("sat_rate1",DoubleType(),True) \
                .add("sat_start1",StringType(),True) \
                .add("sat_end1",StringType(),True) \
                .add("sat_rate2",DoubleType(),True) \
                .add("sat_start2",StringType(),True) \
                .add("sat_end2",StringType(),True) \
                .add("sat_rate3",DoubleType(),True) \
                .add("sat_start3",StringType(),True) \
                .add("sat_end3",StringType(),True) \
                .add("rpz_zone",StringType(),True) \
                .add("rpz_area",DoubleType(),True) \
                .add("paidarea",StringType(),True) \
                .add("parking_time_limit",DoubleType(),True) \
                .add("subarea",StringType(),True) \
                .add("start_time_wkd",StringType(),True) \
                .add("end_time_wkd",StringType(),True) \
                .add("start_time_sat",StringType(),True) \
                .add("end_time_sat",StringType(),True) \
                .add("primarydistrictcd",StringType(),True) \
                .add("secondarydistrictcd",StringType(),True) \
                .add("overrideyn",StringType(),True) \
                .add("overridecomment",IntegerType(),True) \
                .add("shape_length",DoubleType(),True) 

        
        blockface = self.spark.read.format("csv") \
                        .option("header", True) \
                        .schema(schema) \
                        .load(self._load_path+"BlockFace.csv")


        columns_to_drop = ["objectid","segkey",
                        "unitid", "unitid2",
                        "block_id","csm",
                        "load","zone",
                        "total_zones","rpz_zone",
                        "rpz_area","paidarea",
                        "start_time_wkd","end_time_wkd",
                        "start_time_sat","end_time_sat",
                        "primarydistrictcd","secondarydistrictcd",
                        "overrideyn","overridecomment",
                        "shape_length"]

        blockface=blockface.drop(*columns_to_drop)


        blockface=blockface.withColumn('wkd_start1',occupancy_udf.format_minstoHHMMSS('wkd_start1')) \
                         .withColumn('wkd_end1',occupancy_udf.format_minstoHHMMSS('wkd_end1')) \
                         .withColumn('wkd_start2',occupancy_udf.format_minstoHHMMSS('wkd_start2')) \
                         .withColumn('wkd_end2',occupancy_udf.format_minstoHHMMSS('wkd_end2')) \
                         .withColumn('wkd_end3',occupancy_udf.format_minstoHHMMSS('wkd_end3')) \
                         .withColumn('sat_start1',occupancy_udf.format_minstoHHMMSS('sat_start1')) \
                         .withColumn('sat_end1',occupancy_udf.format_minstoHHMMSS('sat_end1')) \
                         .withColumn('sat_start2',occupancy_udf.format_minstoHHMMSS('sat_start2')) \
                         .withColumn('sat_end2',occupancy_udf.format_minstoHHMMSS('sat_end2')) \
                         .withColumn('sat_start3',occupancy_udf.format_minstoHHMMSS('sat_start3')) \
                         .withColumn('sat_end3',occupancy_udf.format_minstoHHMMSS('sat_end3'))
        
        blockface.show(3, truncate=False)
                        
                



