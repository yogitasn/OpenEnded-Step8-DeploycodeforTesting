#### Ingestion
The Seattle Parking Occupancy Dataset are csv files provided as Year to Date data from 2012 to present


#### `occupancy_ingest.py` 
[[Details](https://github.com/yogitasn/Springboard-Step-6-Scale-Your-Prototype/wiki/Data-Pipeline:-Ingestion-Script)] Python script for downloading the files from Seattle Open Data to Azure file share

#### Post Ingestion 
[[Details](https://github.com/yogitasn/Springboard-Step-6-Scale-Your-Prototype/wiki/Post-Ingestion-Step)]  Instructions for moving the files from file share to Azure storage to read them in Pyspark dataframe in the data processing step