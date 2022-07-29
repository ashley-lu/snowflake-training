import pandas as pd
import streamlit as st
import snowflake.connector as sf
from secret import secret
import csv

conn = sf.connect(
        account=secret.get('account'),
        user=secret.get('user'),
        password=secret.get('password'),
        database='REALESTATE_DATA_ATLAS',
        schema='REALESTATE',
        warehouse='COMPUTE_WH',
        role="ACCOUNTADMIN"
)

query = '''SELECT * FROM "REALESTATE"."ZRHVI2020JUL" WHERE ("Region Name" = 'Los Angeles-Long Beach-Anaheim, CA' OR "Region Name" = 'San Diego, CA' OR "Region Name" = 'Chico, CA') AND "Indicator Name" = 'ZHVI - Single Family Residence' ORDER BY "Date"'''

#print(query)
inflation_df = pd.read_sql_query(query, conn)

st.write(inflation_df)



# test