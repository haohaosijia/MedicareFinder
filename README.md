# Provider Prospectus
**[Slides](https://docs.google.com/presentation/d/1fSQX1sfXcJ_6hanYQRTfsz3sDo1U9aSX/edit#slide=id.g9b6a5d037e_0_256)**

A medicare provider and physician comparison platform help you compare:
- Prices of local health care services
- Services provided by specific providers
- Profession and experience of physicians
- National recommended medical protocols provided
## Data Resource
- [Claim Datasets](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF)
- [Provider Datasets](https://data.medicare.gov/Nursing-Home-Compare/Provider-Info/4pq5-n9py)
- [Physician Datasets](https://data.medicare.gov/Physician-Compare/Physician-Compare-National-Downloadable-File/mj5m-pzi6)

## Tech Stack
![Image of Yaktocat](https://github.com/haohaosijia/Provider-Prospectus/blob/master/image/tech_stack.png)
## Installation and Configuration
### Main.py
This file contains code of data processing with Spark. 

Installation configuration:
- Spark Version: spark-2.4.7-bin-hadoop2.7.tgz
- Java Version: openjdk-8-jre-headless
- Python Version: python3.7.9
- Jbdc Driver Version: PostgreSQL JDBC Driver 42.2.17
- Spark configure:
```sh
$ spark-submit --conf spark.driver.maxResultSize=5g --driver-memory 3g --executor-memory 4g --conf spark.shuffle.registration.timeout=50000 --conf spark.sql.shuffle.partitions=1000 --driver-class-path postgresql-42.2.16.jar --jars postgresql-42.2.16.jar --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --master spark://<local  ip>:7077 main.py

```

The detailed use of code includes:
- Read data from S3;
- Combine three different datasets together;
- Write data into postgresql.

Extra configure file used in this script: **db_properties.ini** & **s3_properties.ini**

The format of **db_properties.ini**:

```sh
[postgresql]     
url = <jdbc:postgresql://public_dns/table_name>          
Database = <Database name>  
username = <user name>       
password = <password>    
host = <public_dns:port_number>    
driver = org.postgresql.Driver 
```
The format of **s3_properties.ini**:

```sh
[s3]     
bucket = <bucket name> 
```

### Web.py
This file contains code of my web demo. I use the open-source app **streamlit**.
Same configure file used in this script: **db_properties.ini**.

Installation configuration:
- Install python library sqlalchemy;
- Install python library psycopg2-binary;
- Install python library streamlit;

In Linux, run command:
```sh
$ streamlit run web.py

```

### My_dag.py

This file contains code of automated data processing using **Airflow**. Airflow includes two parts: one is upload file from local to s3, another is batching in spark.

## Challenge
### Loss of Primary Key
The claim dataset I used is a public sample dataset in which private identifier information is coarsened. In order to fill the loss in the primary key column, I do the simulation by assigning real ID in physician dataset to the each claim record in claim datasets. The code is in the main.py.

### Spark Optimization
I have 5 times join operations in my code to achieve the simulation which leads to a large amount of data shuffle. The shuffle is Spark’s mechanism for re-distributing data so that it’s grouped differently across partitions. This typically involves copying data across executors and machines, making the shuffle a complex and costly operation.
I use broadcast hashjoin as the solution. Broadcast variables in Apache Spark is a mechanism for sharing variables across executors that are meant to be read-only. Without broadcast variables these variables would be shipped to each executor for every transformation and action, and this can cause network overhead. However, with broadcast variables, they are shipped once to all executors and are cached for future reference. Also, for my case, the physician and provider datasets are reletively small and could be stored in the driver. So the broadcast hashjoin fits me very well.
 
## Test
### Input
Input file includes a text file which denotes the test file name as input key.

### Output
Output file includes two csv denote two output database: One is providers information database; another is physicians information database.
