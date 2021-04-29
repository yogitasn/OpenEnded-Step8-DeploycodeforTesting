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
from occupancy_processing import job_tracker
import sys
import os
import glob


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
""" Track the etl processing status in POSTGRES table """

def update_control_table(job_id, JOBNAME, status, dataset, loadtype, step, stepdesc, year_processed, date):
    job_tracker.insert_job_details(job_id, JOBNAME, status, dataset, loadtype, step, stepdesc, year_processed, date)


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
        miscProcess.log_error(SCRIPT_NAME, "Error : Reading Job Control file {} ".format(job_control_file),ReturnCode)
        exit(STEP)
    globals().update(paramFile)
else:
    miscProcess.log_error(SCRIPT_NAME, "Job control filename: {} doesn't exist ".format(job_control_file), STEP)
    exit(STEP)


#==============================================================================================================#
(STEP, STEP_DESC)=(20, "Validate All Needed Parameters defined from the control files")
#===============================================================================================================#
# ALWAYS PERFORM THIS STEP
miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP: {}: {}".format(STEP, STEP_DESC))


if 'RerunId' not in globals():
    miscProcess.log_error(SCRIPT_NAME,"ERROR: Parameter RerunId is not defined on control file: {}".format(JOBNAME+".cfg"), STEP)
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

if isinstance('historic_years', list) == False:
    historic_years = ['2014','2015','2016','2017']

print(historic_years)
if isinstance('recent_years', list) == False:
    recent_years = ['2020']

print(historic_years)
print(recent_years)

if StartStep.isnumeric():
    StartStep=int(StartStep)
else:
    miscProcess.log_error(SCRIPT_NAME, "ERROR: Parameter StartStep: {} is not numerics value, check file: {}".format(StartStep,\
                                    job_control_file),STEP)
    exit(STEP)


if StopStep.isnumeric():
    StopStep=int(StopStep)
else:
    miscProcess.log_error(SCRIPT_NAME, "ERROR: Parameter StepStep: {} is not numerics value, check file: {}".format(StopStep,\
                                    job_control_file), STEP)
    exit(STEP)

if max_retry_count.isnumeric():
    max_retry_count=int(max_retry_count)
else:
    miscProcess.log_error(SCRIPT_NAME, "ERROR: Parameter max_retry_delay: {} is not numerics value, check file: {}".format(max_retry_count,\
                                  job_control_file),STEP)
    exit(STEP)

if retry_delay.isnumeric():
    retry_delay=int(retry_delay)
else:
    miscProcess.log_error(SCRIPT_NAME, "ERROR: Parameter retry_delay: {} is not numerics value, check file: {}".format(retry_delay, \
                                 job_control_file), STEP)
    exit(STEP)


#==============================================================================================================#
(STEP, STEP_DESC)=(30, "Read Possible Previous Execution Runtime Control")
#==============================================================================================================#
# ALWAYS PERFORM THIS STEP

miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))

"""
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

#historic_years =['2014', '2015', '2016', '2017']

isHistoric = True

for y in historic_years:
    status = job_tracker.get_historic_job_status(y)
    print("{} for year:{}".format(status,y))
    if status=="Failed":
        miscProcess.log_info(SCRIPT_NAME, "Historical data for the year {}  needs to be re processed ".format(y))
        isHistoric = False


isHistoric1 = True

#recent_years= ['2020']
for y in recent_years:
    status = job_tracker.get_historic_job_status(y)
    print("{} for year:{}".format(status,y))
    if status=="Failed":
        miscProcess.log_info(SCRIPT_NAME, "Historical data for the year {}  needs to be re processed ".format(y))
        isHistoric1 = False



print("Flag for executing Historic data {}".format(isHistoric))
print("Flag for executing Historic data1 {}".format(isHistoric1))

#==============================================================================================================#
(STEP, STEP_DESC)=(40, "Processing Blockface Dataframe configuration file")
#===============================================================================================================#
miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))

#dataframe_config_filename='C:/Step 8 Deploy your code for Testing/2 - data processing/common/blockface.json'

if(StartStep <= STEP and StopStep >=STEP):
    miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}: {} ".format(STEP,STEP_DESC))
    print(blockface_config_filename)
    if os.path.isfile(blockface_config_filename):
        miscProcess.log_info(SCRIPT_NAME, "Blockface Dataframe Configuration filename: {} exists ".format(blockface_config_filename))
        blockface_config_dict = processDataframeConfig.json_reader(blockface_config_filename)
    else:
        miscProcess.log_error(SCRIPT_NAME, "ERROR: Dataframe Configuration file: {} does not exist ".\
                                format(blockface_config_filename), STEP)
        exit(STEP)


# Get Table Column List
cols_list = processDataframeConfig.build_dataframe_column_list(blockface_config_dict)

blockfacefilePath = processDataframeConfig.get_source_driverFilerPath(blockface_config_dict)

# Get Target Table Schema
TargetDataframeSchema = processDataframeConfig.get_dataframe_schema(blockface_config_dict)

OutputPath =  processDataframeConfig.get_source_OutputPath(blockface_config_dict)

if os.path.isdir(OutputPath):
    miscProcess.log_info(SCRIPT_NAME, " Output directory {} exists ".format(OutputPath))
else:
    miscProcess.log_error(SCRIPT_NAME, "ERROR: Output directory: {} does not exist ".format(OutputPath), STEP)
    exit(STEP)

update_control_table(job_id=123, JOBNAME=JOBNAME, status="In Progess",\
                    dataset="Blockface Dataset",loadtype="STATIC", \
                    step=STEP, stepdesc='CompletedStep', 
                    year_processed = '2021', date=datetime.today())

#update_runtime_tracker('CompletedStep', STEP)


#=================================================================
(STEP, STEP_DESC) =(50, "Create Dataframe, Build and Execute Blockface Transformation Process")
#==================================================================
miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))


if(StartStep <= STEP and StopStep >=STEP):  
    print("Inside 50 Step")
    current_time = datetime.now()
    LoadStartTs = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    miscProcess.log_print("LoadStartTs: {}".format(LoadStartTs))
#    update_runtime_tracker('LoadStartTs', LoadStartTs)

    #=================================================================
    # == Create Blockface Dataframe from the sources
    #==================================================================


    (src_df, source_data_info_array) = (None,  None)
   # print("Trying to read into dataframe")
    try:
        (src_df, source_data_info_array) = executeBlockface.sourceBlockfaceReadParquet(blockfacefilePath,TargetDataframeSchema)

        print("Blocface dataframe read")

    except Exception as e:
        miscProcess.log_error(SCRIPT_NAME, "Source Error: {}".format(e), STEP)
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
        miscProcess.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ",STEP)
        update_control_table(job_id=123, JOBNAME=JOBNAME, status="Failed", 
                            dataset="Blockface Dataset",loadtype="STATIC",step=STEP, 
                            stepdesc='FailedStep', year_processed= '2021', date=datetime.today())
        exit(STEP)

    src_df.show(3)
    #update_runtime_tracker('CompletedStep', STEP)
    update_control_table(job_id=123, JOBNAME=JOBNAME, status="Success", dataset="Blockface Dataset",\
                     loadtype="STATIC", step = STEP, stepdesc='CompletedStep', year_processed= '2021', date=datetime.today())



#==============================================================================================================#
(STEP, STEP_DESC)=(60, "Processing Occupancy Dataframe configuration file")
#===============================================================================================================#


if(StartStep <= STEP and StopStep >=STEP):
    miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}: {} ".format(STEP,STEP_DESC))

    if os.path.isfile(occupancy_config_filename):
        miscProcess.log_info(SCRIPT_NAME, "Occupancy Configuration filename: {} exists ".format(occupancy_config_filename))
        occupancy_config_dict = processDataframeConfig.json_reader(occupancy_config_filename)
    else:
        miscProcess.log_error(SCRIPT_NAME, "ERROR: Occupancy Configuration file: {} does not exist ".format(occupancy_config_filename), STEP)
        exit(STEP)


    # Get Dataframe Column List
    OccpnColumnList = processDataframeConfig.build_dataframe_column_list(occupancy_config_dict)
#    update_runtime_tracker('Occupancy_ColumList', OccpnColumnList)

    # Get Column Partition
    PartitionColumn = processDataframeConfig.partition_column(occupancy_config_dict)
 #   update_runtime_tracker('Occupancy_Partition_column', PartitionColumn)

    # Get Target Dataframe Schema
    TargetOccpDFSchema = processDataframeConfig.get_dataframe_schema(occupancy_config_dict)
  #  update_runtime_tracker('OccupancyTargetDFSchema', TargetDFSchema)


    # Get Occupancy dataset File path
    occupancyFilePath = processDataframeConfig.get_source_driverFilerPath(occupancy_config_dict)
  #  update_runtime_tracker('OccupancyFilePath', occupancyFilePath

    OutputPath =  processDataframeConfig.get_source_OutputPath(occupancy_config_dict)

#=================================================================
(STEP, STEP_DESC) =(70, "Create Dataframe, Build and Execute Occupancy Process")
#==================================================================
miscProcess.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))


if(StartStep <= STEP and StopStep >=STEP):  
    print("Inside 70 Step")
    current_time = datetime.now()
    LoadStartTs = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    miscProcess.log_print("LoadStartTs: {}".format(LoadStartTs))
   # update_runtime_tracker('LoadStartTs', LoadStartTs)

    #=================================================================
    # == Create Occupancy Dataframe from the sources
    #==================================================================

    miscProcess.log_info("Processing historical data")
    today = datetime.now()
    current_year = today.year


    file_names = glob.glob(occupancyFilePath)
    
    for file in file_names:
        year = file.split("\\")[3][:4]
        print(year)
        print(isHistoric)
        print(isHistoric1)

        if int(year) >= 2012 and int(year) <=2017 and isHistoric==False:
            (src_df,  source_data_info_array) = (None, None)
            print("Inside historical 2012-2017")
            occupancyFilePath = file
            
            try:
                (src_df, source_data_info_array) = executeOccupancyProcess.sourceOccupancyReadParquet(occupancyFilePath, TargetOccpDFSchema, PartitionColumn)

            except Exception as e:
                miscProcess.log_error(SCRIPT_NAME, "Source Error: {}".format(e), STEP)
                exit(STEP)


            #=================================================================
            # == Create Blockface Transformations on Dataframe
            #==================================================================        
            (ReturnCode, rec_cnt) = executeOccupancyProcess.executeHistoricOccupancyOperations(src_df, OutputPath, OccpnColumnList, PartitionColumn,\
                                                            max_retry_count,retry_delay, TargetOccpDFSchema)
                                                            

            if ReturnCode != 0:
                miscProcess.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ", STEP)
                update_control_table(job_id=123, JOBNAME=JOBNAME, status="Failed", dataset="Occupancy Dataset",\
                            loadtype="HISTORIC", step=STEP, stepdesc='FailedStep', year_processed = year, date=datetime.today())
                exit(STEP)

            update_control_table(job_id=123, JOBNAME=JOBNAME, status="Success", dataset="Occupancy Dataset",\
                            loadtype="HISTORIC", step=STEP, stepdesc='CompletedStep', year_processed = year, date=datetime.today())
        # update_runtime_tracker('CompletedStep', STEP)

        elif int(year) >=2018 and int(year) <=current_year -1 and isHistoric1==False:
            print("Inside year {}".format(year))
            occupancyFilePath = file
            (src_df,  source_data_info_array) = (None, None)
            
            try:
                (src_df, source_data_info_array) = executeOccupancyProcess.sourceOccupancyReadParquet(occupancyFilePath, TargetOccpDFSchema, PartitionColumn)

            except Exception as e:
                miscProcess.log_error(SCRIPT_NAME, "Source Error: {}".format(e), STEP)
                exit(STEP)

            (ReturnCode, rec_cnt) = executeOccupancyProcess.executeOccupancyOperations(src_df, OutputPath, OccpnColumnList, PartitionColumn,\
                                                            max_retry_count,retry_delay)

            if ReturnCode != 0:
                miscProcess.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ", STEP)
                update_control_table(job_id=123, JOBNAME=JOBNAME, status="Failed", dataset="Occupancy Dataset",\
                            loadtype="HISTORIC", step=STEP, stepdesc='FailedStep', year_processed = year, date=datetime.today())
                exit(STEP)

            update_control_table(job_id=123, JOBNAME=JOBNAME, status="Success", dataset="Occupancy Dataset",\
                            loadtype="HISTORIC", step=STEP, stepdesc='CompletedStep',year_processed= year, date=datetime.today())
        else:
            occupancyFilePath = file
            (src_df,  source_data_info_array) = (None, None)
            
            try:
                (src_df, source_data_info_array) = executeOccupancyProcess.sourceOccupancyReadParquet(occupancyFilePath, TargetOccpDFSchema, PartitionColumn)

            except Exception as e:
                miscProcess.log_error(SCRIPT_NAME, "Source Error: {}".format(e), STEP)
                exit(STEP)


            (ReturnCode, rec_cnt) = executeOccupancyProcess.executeOccupancyOperations(src_df, OutputPath, OccpnColumnList, PartitionColumn,\
                                                            max_retry_count,retry_delay)

            if ReturnCode != 0:
                miscProcess.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ", STEP)
                update_control_table(job_id=123, JOBNAME=JOBNAME, status="Failed", dataset="Occupancy Dataset",\
                            loadtype="DELTA", step=STEP, stepdesc='FailedStep', year_processed = year, date=datetime.today())
                exit(STEP)

            update_control_table(job_id=123, JOBNAME=JOBNAME, status="Success", dataset="Occupancy Dataset",\
                            loadtype="DELTA", step=STEP, stepdesc='CompletedStep',year_processed = year, date=datetime.today())
    
    


    

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