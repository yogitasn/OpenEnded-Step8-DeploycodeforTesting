#!/anaconda3/envs/dbconnect/python.exe
import findspark

findspark.init()

findspark.find()

import pyspark

findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType
import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import logging.config
import configparser
import time
from datetime import datetime
from pathlib import Path
from occupancy_processing import miscProcess
from occupancy_processing import processDataframeConfig
from occupancy_processing import executeBlockface
from occupancy_processing import readEnvironmentParameters
from occupancy_processing import executeOccupancyProcess
import sys
import os


path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
logging.info(path)

SCRIPT_NAME= os.path.basename(__file__)

def create_sparksession():
    """
    Initialize a spark session
    """
    return SparkSession.builder.\
           appName("Seattle Parking Occupancy ETL").\
           getOrCreate()

            
#=======================================================================================================#
# INITIALIZATION
#=======================================================================================================#
"""
"""
USAGE = "etlFramework.py <caller_jobname> <log-filename> <blockface-dataframe-name> <occupancy-dataframe-name> <env-path> <spark-client-mode> <user-id>"
num_of_arg_passed = len(sys.argv) - 1
if num_of_arg_passed != 7:
    print("ERROR: Incorrect number of parameter passed, excepted 7 but received: {}".format(num_of_arg_passed))
    print("USAGE: {}".format(USAGE))
    exit(1)

JOBNAME = sys.argv[1]
LogFileName = sys.argv[2]
BlockfaceDataframeName = sys.argv[3]
OccupancyDataframeName = sys.argv[4]
ControlPath = sys.argv[5]
SparkSubmitClientMode = sys.argv[6].upper()
UserId = sys.argv[7]
PROCESS_TYPE = 'POC' 



#=======================================================================================================#
############################ FUNCTIONS #################################################################
#=======================================================================================================#
# Write to Runtime Process Tracker 
tracker_file_name = "C://JobTracker//"+JOBNAME+"_"+UserId+"00"+".json"

# track in RDMS
tracker_dict = {}
def update_runtime_tracker(param, value):
    tracker_dict.update({param: value})
    spark.sparkContext.parallelize([tracker_dict]).toDF().coalesce(1).write.mode('overwrite').json(tracker_file_name)


#=======================================================================================================#
#=== Determine Current Environment ======================================================================
#-======================================================================================================#
"""
RuntimeEnv = socket.gethostname[:3].lower()
if RuntimeEnv not in ['dev','sit','pat']:
    RuntimeEnv='prod'

RuntimeUserId=getpass.getUser().lower()
"""

#=========================================================================================================
#================ Open Spark Context Session =============================================================
#=========================================================================================================

spark = create_sparksession()

# Make the SQLContext Session available to sub-scripts
#executeOccupancyOperations.global_SQLContext(spark)
executeBlockface.global_SQLContext(spark)
executeOccupancyProcess.global_SQLContext(spark)
readEnvironmentParameters.global_SQLContext(spark)
miscProcess.global_SQLContext(spark)


#=========================================================================================================
#================ Initialize log Filename =============================================================
#=========================================================================================================

miscProcess.initial_log_file(LogFileName)


#=========================================================================================================
# PROCESS ALL PARAMETERS
STEP, STEP_DESC=(10, "Read Job Specific Parameter Files")
#=========================================================================================================

miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))

if SparkSubmitClientMode == 'Y':
    # Spark Submitted in Client Mode
    job_control_file = ControlPath + JOBNAME +".cfg"
    blockface_config_filename = ConfigPath +BlockfaceDataframeName.lower()+'.json'
    occupancy_config_filename = ConfigPath +OccupancyDataframeName.lower()+'.json'
else:
    # Spark Submitted in Cluster Mode
    job_control_file = './' + JOBNAME +".cfg"
    blockface_config_filename = './common/' +BlockfaceDataframeName.lower()+'.json'
    occupancy_config_filename = './common/' +OccupancyDataframeName.lower()+'.json'
print(job_control_file)
if os.path.isfile(job_control_file):
    miscProcess.log_info(SCRIPT_NAME, "Job control filename: {} exist".format(job_control_file))
    paramFile, ReturnCode = readEnvironmentParameters.read_job_control(job_control_file)

    if ReturnCode !=0:
        miscProcess.log_error(SCRIPT_NAME, "Error : Reading Job Control file {} {}".format(job_control_file,ReturnCode))
        exit(STEP)
    globals().update(paramFile)
else:
    miscProcess.log_info(SCRIPT_NAME, "Job control filename: {} doesn't exist {}".format(job_control_file, STEP))
    exit(STEP)


#==============================================================================================================#
(STEP, STEP_DESC)=(20, "Validate All Needed Parameters defined from the control files")
#===============================================================================================================#
# ALWAYS PERFORM THIS STEP
miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP: {}: {}".format(STEP, STEP_DESC))

if 'OutputPath' not in globals():
    miscProcess.log_error(SCRIPT_NAME,"ERROR: Parameter OutputPath is not defined on control file: {}".format(JOBNAME+".cfg"))
    exit(STEP)

miscProcess.log_print("OutputPath: {}".format(OutputPath))

if 'RerunId' not in globals():
    miscProcess.log_error(SCRIPT_NAME,"ERROR: Parameter RerunId is not defined on control file: {}".format(JOBNAME+".cfg"))
    exit(STEP)

miscProcess.log_print("OutputPath: {}".format(OutputPath))

if 'ErrorRetryCount' not in globals():
    ErrorRetryCount =1
else:
    ErrorRetryCount=int(ErrorRetryCount)

if 'RetryDelay' not in globals():
    RetryDelay =600
else:
    RetryDelay=int(RetryDelay)


if StartStep.isnumeric():
    StartStep=int(StartStep)
else:
    miscProcess.log_error(SCRIPT_NAME, "ERROR: Parameter StartStep: {} is not numerics value, check file: {}".format(StartStep))
    exit(STEP)


if StopStep.isnumeric():
    StopStep=int(StopStep)
else:
    miscProcess.log_error(SCRIPT_NAME, "ERROR: Parameter StepStep: {} is not numerics value, check file: {}".format(StopStep))
    exit(STEP)

if max_retry_count.isnumeric():
    max_retry_count=int(max_retry_count)
else:
    miscProcess.log_error(SCRIPT_NAME, "ERROR: Parameter max_retry_delay: {} is not numerics value, check file: {}".format(max_retry_count))
    exit(STEP)

if retry_delay.isnumeric():
    retry_delay=int(retry_delay)
else:
    miscProcess.log_error(SCRIPT_NAME, "ERROR: Parameter retry_delay: {} is not numerics value, check file: {}".format(retry_delay))
    exit(STEP)

"""
#==============================================================================================================#
(STEP, STEP_DESC)=(30, "Read Possible Previous Execution Runtime Control")
#==============================================================================================================#
# ALWAYS PERFORM THIS STEP

miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))

tracker_dict={}

if SparkSubmitClientMode == 'Y':
    runtime_tracker_filename = TempPath+'/'+JOBNAME+'_'+RuntimeUserId+'_runtime_tracker_'+'_00'+RerunId
else:
    runtime_tracker_filename = './'+JOBNAME+'_'+RuntimeUserId+'_runtime_tracker_'+'_00'+RerunId

RERUN=False
runtime_tracker_hdfs = TempFolder+'/runtime_tracker_'+'_00'+RerunId
#Verify if the input HDFS file exist

miscProcess.log_print("HDFS Runtime Tracker: {}".format(runtime_tracker_hdfs))
proc = subprocess.Popen(['hadoop','fs','-test','-e', runtime_tracker_hdfs])
proc.communicate()
tracker_rc= proc.returncode 
if tracker_rc == 0:
    miscProcess.log_info(SCRIPT_NAME, "Previous Failed Executed Runtime Tracker Detected: {}".format(runtime_tracker_hdfs))
    miscProcess.log_info(SCRIPT_NAME, "Reloading Parameters Defined from previous failed executions...")

    get_tracker = subprocess.Popen(['hadoop','fs','-get','{}/part-00000.json'.foramt(runtime_tracker_hdfs, runtime_tracker_filename)])

    get_tracker.communicate()

    with open(runtime_tracker_filename,'r') as fpi:
        tracker_dict = json.load(fpi)

    fpi.close()
    miscProcess.log_print(tracker_dict)

    globals().update(tracker_dict)

    if 'CompletedStep' in globals():
        miscProcess.log_info(SCRIPT_NAME, "Last Processed Step on previous execution : {}".format(CompletedStep))
        StartStep = int(CompletedStep) + 1
        miscProcess.log_print("Resuming execution from STEP: {}".format(StartStep))

else:
    miscProcess.log_info(SCRIPT_NAME, "No previous failed executions, running from the top...")

    
"""
#==============================================================================================================#
(STEP, STEP_DESC)=(40, "Processing Blockface Dataframe configuration file")
#===============================================================================================================#
miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))

#dataframe_config_filename='C:/Step 8 Deploy your code for Testing/2 - data processing/common/blockface.json'

if(StartStep <= STEP and StopStep >=STEP):
    miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}: {} ".format(STEP,STEP_DESC))

    if os.path.isfile(blockface_config_filename):
        miscProcess.log_info(SCRIPT_NAME, "Blockface Dataframe Configuration filename: {} exists ".format(blockface_config_filename))
        blockface_config_dict = processDataframeConfig.json_reader(blockface_config_filename)
        #update_runtime_tracker('blockface_config_dict', blockface_config_dict)
    else:
        miscProcess.log_error(SCRIPT_NAME, "ERROR: Dataframe Configuration file: {} does not exist ".\
                                format(blockface_config_filename, STEP))
        exit(STEP)


# Get Table Column List
cols_list = processDataframeConfig.build_dataframe_column_list(blockface_config_dict)

update_runtime_tracker('ColumnList', cols_list)

blockfacefilePath = processDataframeConfig.get_source_driverFilerPath(blockface_config_dict)

update_runtime_tracker("BlockfacefilePath", blockfacefilePath)

# Get Target Table Schema
TargetDataframeSchema = processDataframeConfig.get_dataframe_schema(blockface_config_dict)


update_runtime_tracker('CompletedStep', STEP)
#=================================================================
(STEP, STEP_DESC) =(50, "Create Dataframe, Build and Execute Blockface Transformation Process")
#==================================================================
miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))


if(StartStep <= STEP and StopStep >=STEP):  
    print("Inside 50 Step")
    # Record time before the load
    # Get the current system timestamp
    current_time = datetime.now()
    LoadStartTs = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    miscProcess.log_print("LoadStartTs: {}".format(LoadStartTs))
    update_runtime_tracker('LoadStartTs', LoadStartTs)

    #=================================================================
    # == Create Blockface Dataframe from the sources
    #==================================================================


    (src_df, source_data_info_array) = (None,  None)
   # print("Trying to read into dataframe")
    try:
        (src_df, source_data_info_array) = executeBlockface.sourceBlockfaceReadParquet(blockfacefilePath,TargetDataframeSchema)

        print("Blocface dataframe read")

    except Exception as e:
        miscProcess.log_error(SCRIPT_NAME, "Source Error: {}".format(e))
        exit(STEP)

    print(source_data_info_array)
    src_df.head(3)
    print("src df schema")
    src_df.printSchema()
    #=================================================================
    # == Create Blockface Transformations on Dataframe
    #==================================================================
    print("Output path is {}".format(OutputPath))
    print("max retry delay {}".format(max_retry_count))
    print("retry delay is {}".format(retry_delay))
    (ReturnCode, rec_cnt) = executeBlockface.executeBlockfaceOperations(src_df, OutputPath, cols_list, max_retry_count,retry_delay)

    if ReturnCode != 0:
        miscProcess.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ")

    src_df.show(3)
    update_runtime_tracker('CompletedStep', STEP)



#==============================================================================================================#
(STEP, STEP_DESC)=(60, "Processing Occupancy Dataframe configuration file")
#===============================================================================================================#


if(StartStep <= STEP and StopStep >=STEP):
    miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}: {} ".format(STEP,STEP_DESC))

    if os.path.isfile(occupancy_config_filename):
        miscProcess.log_info(SCRIPT_NAME, "Occupancy Configuration filename: {} exists ".format(occupancy_config_filename))
        occupancy_config_dict = processDataframeConfig.json_reader(occupancy_config_filename)
    else:
        miscProcess.log_error(SCRIPT_NAME, "ERROR: Occupancy Configuration file: {} does not exist ".format(occupancy_config_filename, STEP))
        exit(STEP)

    
   # TableLoadFreq_lcase = processDataframeConfig.load_freq(occupancy_config_dict).lower()
   # miscProcess.log_info(SCRIPT_NAME, 'Table Load Freq: {0}'.format(occupancy_config_dict))

    # Get Dataframe Column List
    OccpnColumnList = processDataframeConfig.build_dataframe_column_list(occupancy_config_dict)
    update_runtime_tracker('Occupancy_ColumList', OccpnColumnList)

    # Get Column Partition
    PartitionColumn = processDataframeConfig.partition_column(occupancy_config_dict)
    update_runtime_tracker('Occupancy_Partition_column', PartitionColumn)

    # Get Target Dataframe Schema
    TargetOccpDFSchema = processDataframeConfig.get_dataframe_schema(occupancy_config_dict)
  #  update_runtime_tracker('OccupancyTargetDFSchema', TargetDFSchema)


    # Get Occupancy dataset File path
    occupancyFilePath = processDataframeConfig.get_source_driverFilerPath(occupancy_config_dict)
    update_runtime_tracker('OccupancyFilePath', occupancyFilePath)


#=================================================================
(STEP, STEP_DESC) =(70, "Create Dataframe, Build and Execute Occupancy Process")
#==================================================================
miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))


if(StartStep <= STEP and StopStep >=STEP):  
    print("Inside 70 Step")
    current_time = datetime.now()
    LoadStartTs = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    miscProcess.log_print("LoadStartTs: {}".format(LoadStartTs))
    update_runtime_tracker('LoadStartTs', LoadStartTs)

    #=================================================================
    # == Create Occupancy Dataframe from the sources
    #==================================================================


    (src_df,  source_data_info_array) = (None, None)

    try:
        (src_df, source_data_info_array) = executeOccupancyProcess.sourceOccupancyReadParquet(occupancyFilePath, TargetOccpDFSchema, PartitionColumn)

    except Exception as e:
        miscProcess.log_error(SCRIPT_NAME, "Source Error: {}".format(e))
        exit(STEP)

    print(source_data_info_array)
    src_df.head(3)
    print("occupancy df schema")
    src_df.printSchema()
    #=================================================================
    # == Create Blockface Transformations on Dataframe
    #==================================================================
    print("Output path is {}".format(OutputPath))
    print("max retry delay {}".format(max_retry_count))
    print("retry delay is {}".format(retry_delay))
    (ReturnCode, rec_cnt) = executeOccupancyProcess.executeOccupancyOperations(src_df, OutputPath, OccpnColumnList, PartitionColumn,\
                                                    max_retry_count,retry_delay)

    if ReturnCode != 0:
        miscProcess.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ")

    src_df.show(3)
    update_runtime_tracker('CompletedStep', STEP)

    miscProcess.complete_log_file()


"""
    


HISTORIC = FALSE

if NOT HISTORIC and year < current_year -1 :
    (ReturnCode, rec_cnt) = executeTranslateProcess.executeOccupancyOperations(tgt_config_source, source_col_map, src_df, TempHDFSStage_file,\
                            PartitionColumn, EffectvDt, ErrorRetryCount, RetryDelay)
    (ReturnCode, rec_cnt) = executeTranslateProcess.executeOccupancyOperations(tgt_config_source, source_col_map, src_df, TempHDFSStage_file,\
                            PartitionColumn, EffectvDt, ErrorRetryCount, RetryDelay)
else:
    (ReturnCode, rec_cnt) = executeTranslateProcess.executeOccupancyOperations(tgt_config_source, source_col_map, src_df, TempHDFSStage_file,\
                                PartitionColumn, EffectvDt, ErrorRetryCount, RetryDelay)

if ReturnCode != 0:
    miscProcess.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ")


    

    
# Entry point for the pipeline
if __name__ == "__main__":
    #main()
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--job", type=str)
    parser.add_argument("--date_range", default=",")

    # Parse the cmd line args
    args = parser.parse_args()
    args = vars(args)
   # logging.info("Cmd line args:\n{}".format(json.dumps(args, sort_keys=True, indent=4)))

    main(**args)

    occupancy_processing_year = config.get('PRODUCTION', 'occupancy_processing_year')

    trade_date = "2012,2017"
    
    
    logging.info("ALL DONE!\n")
"""