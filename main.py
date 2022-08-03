import pandas as pd
import streamlit as st
import snowflake.connector as sf
from secret import secret
import csv
import numpy as np
import altair as alt
from statemap import statemap

@st.experimental_singleton
def init_connection():
        return sf.connect(
        account=secret.get('account'),
        user=secret.get('user'),
        password=secret.get('password'),
        database='REALESTATE_DATA_ATLAS',
        schema='REALESTATE',
        warehouse='COMPUTE_WH',
        role="ACCOUNTADMIN"
)

conn = init_connection()

#query = '''SELECT * FROM "REALESTATE"."ZRHVI2020JUL" WHERE ("Region Name" = 'Los Angeles-Long Beach-Anaheim, CA' OR "Region Name" = 'San Diego, CA' OR "Region Name" = 'Chico, CA') AND "Indicator Name" = 'ZHVI - Single Family Residence' ORDER BY "Date"'''



stateQuery = """select trim(substr("Region Name",len("Region Name")-2,3)) as STATE
from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
group by substr("Region Name",len("Region Name")-2,3)
order by substr("Region Name",len("Region Name")-2,3)
limit 51
;"""

states = pd.read_sql_query(stateQuery, conn)

col1, col2 = st.columns(2)

with col1:
        my_expander = st.expander(label='Select State')
        with my_expander:
                selectedState1 = st.selectbox("Which State?", states, key=126)
with col2:
        my_expander2 = st.expander(label='Select State')
        with my_expander2:
                selectedState2 = st.selectbox("Which State?", states, key=125)


query1 = f"""SELECT DISTINCT "Region Name" FROM ZRHVI2020JUL WHERE "Region Name" LIKE '%{selectedState1}';"""  
query2 = f"""SELECT DISTINCT "Region Name" FROM ZRHVI2020JUL WHERE "Region Name" LIKE '%{selectedState2}';"""        





cities1 = pd.read_sql_query(query1, conn)
cities2 = pd.read_sql_query(query2, conn)





        
# Holds  Strings of city names 
selectedCities = []


# City dropdowns
with col1:
        option = st.selectbox(
        'Which Data would you like to see?',
        cities1, key=123)
        selectedCities.append(str(option))
        st.write('You selected:', option)


with col2:
        option = st.selectbox(
        'Which other Data would you like to see?',
        cities2, key=124)
        selectedCities.append(str(option))
        st.write('You selected:', option)



# Pandas dataframes with price, date and location as columns
dfList = []

for city in selectedCities:
        query = f"""select "Date", round(avg("Value"),0) as "Value"
        from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
        where "Region Name" like '{city}' and "Date" >= to_date('01/01/1900','mm/dd/yyyy')
        group by "Region Name", "Date"
        order by "Region Name", "Date"
        ;"""
        result = pd.read_sql_query(query,conn)
        
        # Creates new dataframe and add location column
        dfList.append(pd.DataFrame({
    "Price ($)":  result.loc[:, "Value"],
    "Date": result.loc[:, "Date"],
    "Location": city
}))


# Merge all dataframes into 1 dataframe
df_combined = pd.concat(dfList)


line_chart = alt.Chart(df_combined).mark_line().encode(
        y=  alt.Y('Price ($)', title='Average Sell Price($)'),
        x=  alt.X( 'Date', title='Date'),
        color = "Location",
    ).properties(
        height=400, width=700,
        title="Housing Price Trend"
    ).configure_title(
        fontSize=16
    )
 
st.altair_chart(line_chart, use_container_width=True)

