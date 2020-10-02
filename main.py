# -*- coding: utf-8 -*-
"""
Created on Fri Oct  2 16:33:19 2020

@author: Nancy~
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType

# Function to read csv from s3 to spark
def read_s3_csv(key):
    s3file = f's3a://{bucket}/{key}'
    df = spark.read.option("header",True).csv(s3file)
    return df


# function for ICD9 CODE match with ICD10 text 
def ICD_to_Text(key1, key2):
    s3file1 = f's3a://{bucket}/{key1}'
    s3file2 = f's3a://{bucket}/{key2}'

    line1 = sc.textFile(s3file1)
    line2 = sc.textFile(s3file2)
    rd1 = line1.map(lambda x: x.split(" ", 1)) \
               .map(lambda x: [i.strip() for i in x])
    rd2 = line2.map(lambda x: x.split(" ")) \
               .map(lambda x: [i.strip() for i in x]) \
               .map(lambda x: list(filter(lambda a: a != '', x)))

    df1 = rd1.toDF(['I10CODE','I10TEXT'])
    df2 = rd2.toDF(['I9CODE','I10CODE','OTHER']).select('I9CODE','I10CODE')
    df3 = df2.join(df1, df2.I10CODE == df1.I10CODE, how = 'left') \
        .select(
            df2.I9CODE
            , df1.I10TEXT
            )
    return df3


# buckey and key name
    
bucket = 'medicareclaim'
key_bs = 'DE1_0_200*_Beneficiary_Summary_File_Sample_*.csv'
key_ip = 'DE1_0_2008_to_2010_Inpatient_Claims_Sample_*.csv'
key_op = 'DE1_0_2008_to_2010_Outpatient_Claims_Sample_*.csv'
key_ca = 'DE1_0_2008_to_2010_Carrier_Claims_Sample_*.csv'
key_phy = 'Physician_Compare_National_Downloadable_File.csv'
key_pos = 'Provider_of_Services_File_-_OTHER_-_June_2020.csv'
key_IDC10 = 'I10cm_desc2010.txt'
key_I9toI10 = '2010_I9gem.txt'

# spark configuration set up

sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')

spark = SparkSession(sc)

# physician dataset input

df_phy = read_s3_csv(key_phy) \
    .select(
        'NPI'
        , 'First Name'
        , 'Last Name'
        , 'Credential'
        , 'Hospital affiliation CCN 1'
        )

# provider dataset input

df_pos = read_s3_csv(key_pos) \
    .select(
        'SSA_CNTY_CD'
        , 'FAC_NAME'
        , 'PRVDR_NUM'
        , 'SSA_STATE_CD'
        , 'ST_ADR'
        , 'ZIP_CD'
        , 'CRTFD_BED_CNT'
        )

# connect provider datasets with physician datasets by inner join

df_phy_pos = df_phy \
    .join(df_pos
          , df_phy['Hospital affiliation CCN 1'] == df_pos.PRVDR_NUM) \
    .withColumn('row_num'
                , row_number().over(Window.partitionBy(
                                            'SSA_STATE_CD'
                                            ,'SSA_CNTY_CD') \
                                          .orderBy('NPI'))) \
    .select(
        'row_num'
        , 'NPI'
        , 'First Name'
        , 'Last Name'
        , 'Credential'
        , df_pos.SSA_CNTY_CD
        , df_pos.FAC_NAME
        , df_pos.SSA_STATE_CD
        , df_pos.PRVDR_NUM
        , df_pos.ST_ADR
        , df_pos.ZIP_CD
        , df_pos.CRTFD_BED_CNT
        )

df_phy_pos.show()

# claim datasets input

# Beneficiary_Summary input

df_bs = read_s3_csv(key_bs) \
    .select(
        'DESYNPUF_ID'
        , 'SP_STATE_CODE'
        , 'BENE_COUNTY_CD'
        )

# impatient claims input

df_ip_claims = read_s3_csv(key_ip)
# combine several diagonosis code columns name together into one list
DGNS_CD_ip_Col = df_ip_claims \
    .select('ADMTNG_ICD9_DGNS_CD'
            , df_ip_claims.colRegex("`(ICD9_DGNS_CD_)\d+`")).columns
# change type
df_ip_claims = df_ip_claims \
    .withColumn('CLM_PMT_AMT'
                , df_ip_claims['CLM_PMT_AMT'].cast(DoubleType())) \
    .withColumn('NCH_BENE_DDCTBL_AMT'
                , df_ip_claims['NCH_BENE_IP_DDCTBL_AMT'].cast(DoubleType())) \
    .withColumn('CLM_UTLZTN_DAY_CNT'
                , df_ip_claims['CLM_UTLZTN_DAY_CNT'].cast(IntegerType())) \
    .withColumn("ICD9_DGNS_CD", f.array(DGNS_CD_ip_Col)) \
    .select(
        'DESYNPUF_ID'
        , 'CLM_ID'
        , 'CLM_PMT_AMT'
        , 'NCH_BENE_DDCTBL_AMT'
        , 'CLM_UTLZTN_DAY_CNT'
#       , 'ADMTNG_ICD9_DGNS_CD'
        , 'ICD9_DGNS_CD'
        )
# df_ip_claims.show()
# df_ip_claims.printSchema()


# outpatient claims input

df_op_claims = read_s3_csv(key_op)
# combine several diagonosis code columns name together into one list
DGNS_CD_op_Col = df_op_claims \
    .select('ADMTNG_ICD9_DGNS_CD'
            , df_op_claims.colRegex("`(ICD9_DGNS_CD_)\d+`")).columns
df_op_claims = df_op_claims \
    .withColumn('CLM_UTLZTN_DAY_CNT', lit(0)) \
    .withColumn('CLM_PMT_AMT'
                , df_op_claims['CLM_PMT_AMT'].cast(DoubleType())) \
    .withColumn('NCH_BENE_DDCTBL_AMT'
                , df_op_claims['NCH_BENE_PTB_DDCTBL_AMT'].cast(DoubleType())) \
    .withColumn("ICD9_DGNS_CD", f.array(DGNS_CD_op_Col)) \
    .select(
        'DESYNPUF_ID'
        , 'CLM_ID'
        , 'CLM_PMT_AMT'
        , 'NCH_BENE_DDCTBL_AMT'
        , 'CLM_UTLZTN_DAY_CNT'
#       , 'ADMTNG_ICD9_DGNS_CD'
        , 'ICD9_DGNS_CD'
        )
# df_op_claims.show()
# df_op_claims.printSchema()

# carrier claims input

df_carrier_claims = read_s3_csv(key_ca)

# combine several similar columns name together into one list
LINE_BENE_PTB_DDCTBL_AMT_Col = df_carrier_claims \
    .select(df_carrier_claims.colRegex("`(LINE_BENE_PTB_DDCTBL_AMT_)\d+`")).columns
LINE_NCH_PMT_AMT_Col = df_carrier_claims \
    .select(df_carrier_claims.colRegex("`(LINE_NCH_PMT_AMT_)\d+`")).columns
DGNS_CD_ca_Col = df_carrier_claims \
    .select(df_carrier_claims.colRegex("`(ICD9_DGNS_CD_)\d+`")).columns

# change columns type
for c in LINE_BENE_PTB_DDCTBL_AMT_Col:
    df_carrier_claims = df_carrier_claims \
        .withColumn(c, df_carrier_claims[c].cast(DoubleType()))
for c in LINE_NCH_PMT_AMT_Col:
    df_carrier_claims = df_carrier_claims \
        .withColumn(c, df_carrier_claims[c].cast(DoubleType()))
df_carrier_claims.printSchema()


# df_ca_line_bene = df_ca_line_bene. \
#     withColumn('TOTAL_LINE_BENE_PTB_DDCTBL_AMT'
#                 , expr('+'.join(LINE_BENE_PTB_DDCTBL_AMT_Col)))
       
# df_ca_line_nch = df_carrier_claims. \
#     select(*(col(c).cast("float").alias(c) for c in LINE_NCH_PMT_AMT_Col))
# df_ca_line_nch = df_ca_line_nch. \
#     withColumn('TOTAL_LINE_NCH_PMT_AMT'
#                 , expr('+'.join(LINE_NCH_PMT_AMT_Col)))

# add new columns to caculate total payment
df_ca_claims = df_carrier_claims \
    .withColumn('NCH_BENE_DDCTBL_AMT'
                , expr('+'.join(LINE_BENE_PTB_DDCTBL_AMT_Col))) \
    .withColumn('CLM_PMT_AMT'
                , expr('+'.join(LINE_NCH_PMT_AMT_Col))) \
    .withColumn("ICD9_DGNS_CD", f.array(DGNS_CD_ca_Col)) \
    .withColumn('CLM_UTLZTN_DAY_CNT', lit(-1)) \
    .select(
        'DESYNPUF_ID'
        , 'CLM_ID'
        , 'CLM_PMT_AMT'
        , 'NCH_BENE_DDCTBL_AMT'
        , 'CLM_UTLZTN_DAY_CNT'
#       , 'ADMTNG_ICD9_DGNS_CD'
        , 'ICD9_DGNS_CD'
        )            
df_ca_claims.show()
df_ca_claims.printSchema()

# unioin three types of claims datasets together

    
df_claims = df_ip_claims.union(df_op_claims) \
    .union(df_ca_claims) \
    .dropDuplicates(['CLM_ID'])
# df_claims.printSchema()

# innerjoin claims datasets with Beneficiary datasets, add state information

df_bc = df_claims.join(df_bs, df_claims.DESYNPUF_ID == df_bs.DESYNPUF_ID) \
    .withColumn('ICD9_DGNS_CD', explode('ICD9_DGNS_CD')) \
    .select(
#        df_bs.DESYNPUF_ID
#         'CLM_ID'
          df_bs.SP_STATE_CODE
        , df_bs.BENE_COUNTY_CD
        , 'CLM_PMT_AMT'
        , 'NCH_BENE_DDCTBL_AMT'
        , 'CLM_UTLZTN_DAY_CNT'
        , 'ICD9_DGNS_CD'
        )
# df_bc.printSchema()

# relpace diagnosis code with text information

df_IDC9 = ICD_to_Text(key_IDC10, key_I9toI10)

# Add text information for ICD9 CODE
df_Ibc = df_bc.join(df_IDC9
                    , df_bc.ICD9_DGNS_CD == df_IDC9.I9CODE
                    , how = 'left') \
    .select(
        'SP_STATE_CODE'
        , 'BENE_COUNTY_CD'
        , 'CLM_PMT_AMT'
        , 'NCH_BENE_DDCTBL_AMT'
        , 'CLM_UTLZTN_DAY_CNT'
#        , 'ICD9_DGNS_CD'
        , 'I10TEXT'
        ).cache()
# df_Ibc.printSchema()
# df_Ibc.show()

# random assign physicians to claim datasets

# Count number of physicians by state and county in order to random assign
# physicians to claim records

df_phy_count = df_phy_pos \
    .select(
        'SSA_STATE_CD'
        , 'SSA_CNTY_CD'
        , 'NPI'
        ) \
    .groupBy('SSA_STATE_CD', 'SSA_CNTY_CD') \
    .agg(countDistinct('NPI').alias('Count'))
# df_phy_count.show()

# left join by row_num, one is using count to generate 
# another use randmon function

df_re_count = df_Ibc.join(
    df_phy_count
    ,((df_bc.SP_STATE_CODE == df_phy_count.SSA_STATE_CD) \
      & (df_bc.BENE_COUNTY_CD == df_phy_count.SSA_CNTY_CD))
    , how = "left") \
    .select(
        df_phy_count.SSA_STATE_CD
        , df_phy_count.SSA_CNTY_CD
        , 'CLM_PMT_AMT'
        , 'NCH_BENE_DDCTBL_AMT'
#        , 'CLM_UTLZTN_DAY_CNT'
        , 'Count'
#        , 'I10TEXT'
        ) \
    .withColumn('row_num'
                , (f.rand()*col('Count') + 1).cast(IntegerType()))
# df_re_count.show() 

# get the random selected physcian npi by left join

df_re = df_re_count.join(
     df_phy_pos
     , ((df_re_count.SSA_STATE_CD == df_phy_pos.SSA_STATE_CD) \
        & (df_re_count.SSA_CNTY_CD == df_phy_pos.SSA_CNTY_CD) \
            & (df_re_count.row_num ==df_phy_pos.row_num))
     , how = 'left') \
    # .select(
    #     df_re_count.SSA_STATE_CD,
    #     df_re_count.SSA_CNTY_CD,
    #     df_re_count.CLM_PMT_AMT,
    #     df_re_count.NCH_BENE_DDCTBL_AMT
    #     , df_phy_pos.NPI
    #     , df_phy_pos['First Name']
    #     , df_phy_pos['Last Name']
    #     , df_phy_pos['Credential']
    #     , df_phy_pos.FAC_NAME
    #     , df_phy_pos.PRVDR_NUM
    #     , df_phy_pos.ST_ADR
    #     , df_phy_pos.ZIP_CD
    #     , df_phy_pos.CRTFD_BED_CNT)  
# df_re.show() 

# Group by physycians id
    
df_phy_groupby = df_re \
    .groupBy('NPI'
             , 'First Name'
             , 'Last Name'
             , 'FAC_NAME') \
        .count() \
    .withColumnRenamed('count', 'Total Number of People Served')
# df_phy_groupby.show()

# Group by providers id
    
df_pos_groupby = df_re \
    .groupBy('PRVDR_NUM'
             , 'ZIP_CD'
             , 'FAC_NAME'
             , 'ST_ADR'
             , 'CRTFD_BED_CNT') \
        .avg('CLM_PMT_AMT', 'NCH_BENE_DDCTBL_AMT')
# df_pos_groupby.show()
    
# read data into postgresql

db_properties={}
config = configparser.ConfigParser()
config.read("db_properties.ini")
db_prop = config['postgresql']
db_url = db_prop['url']
db_properties['user']=db_prop['username']
# print(db_properties['username'])
db_properties['password']=db_prop['password']
# db_properties['url']=
db_properties['driver']=db_prop['driver']
# print(db_properties)
# df_bc.write.jdbc(url=db_url,table='bc',mode='overwrite',properties=db_properties)
df_phy_groupby.write.jdbc(url=db_url,table='phy',mode='overwrite',properties=db_properties)
df_pos_groupby.write.jdbc(url=db_url,table='pos',mode='overwrite',properties=db_properties)
