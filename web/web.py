# -*- coding: utf-8 -*-
"""
Created on Mon Oct  5 19:23:18 2020

@author: Nancy~
"""
from sqlalchemy import create_engine
import streamlit as st
import pandas as pd
import altair as alt
import configparser

@st.cache

# Get the data from postgresql database
def get_pos_data():
    config = configparser.ConfigParser()
    config.read("db_properties.ini")
    db_prop = config['postgresql']
    db_user = db_prop['username']
    db_password = db_prop['password']
    db_host = db_prop['host']
    db_table = 'nancy'
    engine = create_engine(f'postgres://{db_user}:{db_password}@{db_host}/{db_table}')
    df1 = pd.read_sql_query('select * from "pos"',con=engine)
    df2 = pd.read_sql_query('select * from "phy" limit 1000000',con=engine)
    return df1.set_index("Zip Code"), df2.set_index('Hospital affiliation')


# output title
st.write("# Provider Prospectus")
df_pos, df_phy = get_pos_data()

# output multiselect char
zip_code = st.multiselect(
    "Search by Zip Code", list(df_pos.sort_index().index),default=['77030']
)

# judge whether zip code exist or not
if not zip_code:
    st.error("Zip Code Unvalid")


# output provider information
data_pos = df_pos.loc[zip_code]
st.write("### Providers Information", data_pos.sort_index())

# output multiselect char
fac_name = st.multiselect(
    "Search by Hospitals Name", list(data_pos.set_index('Name')
.index))

# judge whether hospital name exist or not
if not fac_name:
    st.error("Hospitals Name Not Found")

# output physician information
data_phy = df_phy.loc[fac_name]
st.write("### Physicians Information", data_phy.sort_index())

