# Provider Prospectus
A medicare provider and physician comparison platform help you compare:
- Prices of local health care services
- Services provided by specific providers
- Profession and experience of physicians
- National recommended medical protocols provided

## Main.py
This file contains code of data processing with Spark. 

Installation configuration:
- Spark Version: spark-2.4.7-bin-hadoop2.7.tgz
- Java Version: openjdk-8-jre-headless
- Python Version: python3.7.9
- Jbdc Driver Version: PostgreSQL JDBC Driver 42.2.17

The detailed use of code includes:
- Read data from S3;
- Combine three different datasets together;
- Write data into postgresql.

Extra configure file used in this script: [db_properties.ini]

The format of [db_properties.ini]:

> [postgresql]     
> url = \<postgresql url>          
> Database = \<Database name>  
> username = \<user name>       
> password = \<password>    
> driver = org.postgresql.Driver 


## Web.py
This file contains code of my web demo. I use the open-source app [streamlit].

Installation configuration:
- Install python library sqlalchemy;
- Install python library psycopg2-binary;
- Install python library streamlit;

In Linux, run command:
```sh
$ streamlit run web.py

```

## My_dag.py


