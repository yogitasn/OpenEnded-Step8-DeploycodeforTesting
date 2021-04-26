#### Data Processing

These scripts are used to extract and transform paid parking occupancy data for the year 2012 to present and blocface to create paid parking fact, date and blockface dimension.

All of the above are done in PySpark. 

#### `databricks.py`
Standalone script to mount the data from Azure container to Databricks

#### `occupancy_transform.py`
PySpark script for transforming data stored in Azure container i.e. the Paid Parking data from '2012 to present' and Seattle Blockface data.

#### `occupancy_etl.py`
Driver PySpark script to trigger the transformation script for the above

#### `occupancy_udf.py` 
UDF to get the data records in HH:MM:SS format.



#### Set up Databricks dev env at local windows
Provision a Databricks cluster

![Alt text](Screenshot/Databricks_cluster.PNG?raw=true "DatabricksCluster")

```
Follow the instructions in the below URL and setup data bricks-connect that enables pyspark code on the local machine to be executed on Databricks cluster
* Reference: https://docs.databricks.com/dev-tools/databricks-connect.html
    * Your Spark job is planned locally but executed on the remote cluster
    * Allow the user to step through and debug Spark code in the local environment

* data bricks-connect==7.3.5 (Matching with the cluster type of 7.3.1 LTS)
* Configuration
    * The trick is one cannot mess up the delicate databricks-connect and pyspark versions
    * The Python version on local and databricks cluster should match i.e. Python 3.7.5


* Test with this example:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print(spark.range(100).count())  # it executes on the cluster, where you can see the record

```


Car parking has been a major issue in urban areas worldwide. Most countries are facing issues related to the lack of parking places. With the increasing economic development and urbanisation, car ownerships are growing rapidly, which exacerbates the imbalance between parking supply and demand [1]. The Ministry of Public Security of China released data of car ownership nationwide in 2018, showing that the number of cars reached 240 million with an annual growth rate of 10.51%, but the total number of parking spaces was only 102.5 million including private and public parking spaces, which is lower than half of the total number of cars. Moreover, around 30% of the traffic congestion in Chongqing and Shanghai, major cities of China, is due to lack of car parking spaces [2]. This issue is mainly caused by ineffective parking management. According to the latest research report [3], the parking space utilisation rate of more than 90% of cities in China is <50%. With the limited areas in the cities, increasing parking area would not be a sustainable solution, but the implementation of efficient parking management would be a practical solution. The intelligent parking system is an essential part of efficient parking management. In intelligent parking system, the time-sensitive parking occupancy prediction will be of great significance for decision makers and city planners regarding parking.

The number of available parking spaces plays an important role in drivers’ decision-making processes regarding parking [4, 5]. According to Caicedo et al. [6], drivers that possess information on parking availability are 45% more successful in availing parking spaces than those without knowledge. Moreover, the parking occupancy prediction is helpful in transportation management and planning [7]. For instance, public agencies such as city traffic and planning departments use the predicted parking occupancy information to manage transportation demand and traffic congestion [8]. Parking facility managers and operators may foresee the parking system performance and carry out short- and long-term preventive strategic decisions to avoid system breakdowns [9]. On the other side, the parking occupancy prediction can help reduce traffic congestion and energy consumption [10]. According to a report [11], on an average, US drivers spend 17 h per year searching for parking spaces at a cost of $345 per driver incurred due to time consumption, fuel, and emissions. If an accurate prediction of parking availability


#### Execute the ETL script and trigger the transformation on the datasets via command line

```
python occupancy_etl.py

```

Final Execution Tables:

![Alt text](Screenshot/DataframeTables.PNG?raw=true "DataFrameTables")


![Alt text](Screenshot/DataframeTables_1.PNG?raw=true "DataFrameTables")



### Spark Jobs 

![Alt text](Screenshot/spark_job_ui.PNG?raw=true "SparkJobUI")
