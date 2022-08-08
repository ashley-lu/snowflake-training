import pandas as pd
import streamlit as st
import snowflake.connector as sf
from secret import secret
import numpy as np
import altair as alt
from statemap import statemap
import matplotlib.pyplot as plt

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


#st.header("U.S Housing Price Comparison Tool")

tab1, tab2 = st.tabs(["Compare Two Locations", "View One Location"])

with tab1:

        stateQuery = """select trim(substr("Region Name",len("Region Name")-2,3)) as STATE
        from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
        group by substr("Region Name",len("Region Name")-2,3)
        order by substr("Region Name",len("Region Name")-2,3)
        limit 51
        ;"""

        states = pd.read_sql_query(stateQuery, conn)

        col1, col2 = st.columns(2)

        with col1:
                selectedState1 = st.selectbox("Select State", states, key=126)
        with col2:
                selectedState2 = st.selectbox("Select State", states, key=125)


        query1 = f"""SELECT DISTINCT "Region Name" FROM ZRHVI2020JUL WHERE "Region Name" LIKE '%{selectedState1}';"""  
        query2 = f"""SELECT DISTINCT "Region Name" FROM ZRHVI2020JUL WHERE "Region Name" LIKE '%{selectedState2}';"""        



        cities1 = pd.read_sql_query(query1, conn)
        cities2 = pd.read_sql_query(query2, conn)

        default_selection = pd.DataFrame({"Region Name":"None"}, index = [0])

        cities1 = pd.concat([default_selection, cities1]).reset_index(drop = True)
        cities2 = pd.concat([default_selection, cities2]).reset_index(drop = True)

                
        # Holds  Strings of city names 
        selectedCities = []


        # City dropdowns
        with col1:
                option = st.selectbox(
                'Select City',
                cities1, key=123)
                selectedCities.append(str(option))
                st.write('You selected:', option)


        with col2:
                option = st.selectbox(
                'Select City',
                cities2, key=124)
                selectedCities.append(str(option))
                st.write('You selected:', option)



        # Pandas dataframes with price, date and location as columns
        dfList = []

        flag = False
        location = None

        for city in selectedCities:
                if not flag and city == "None":
                        query = f"""select right(trim("Region Name"),2) as STATE, "Date", round(avg("Value"),0) as "Value"
                        from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
                        where "Region Name" like '%{selectedState1}' and "Date" >= to_date('01/01/1900','mm/dd/yyyy')
                        group by right(trim("Region Name"),2), "Date"
                        order by right(trim("Region Name"),2), "Date"
                        ;"""
                        location = selectedState1
                elif flag and city == "None":
                        query = f"""select right(trim("Region Name"),2) as STATE, "Date", round(avg("Value"),0) as "Value"
                        from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
                        where "Region Name" like '%{selectedState2}' and "Date" >= to_date('01/01/1900','mm/dd/yyyy')
                        group by right(trim("Region Name"),2), "Date"
                        order by right(trim("Region Name"),2), "Date"
                        ;"""
                        location = selectedState2
                else:
                        query = f"""select "Date", round(avg("Value"),0) as "Value"
                        from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
                        where "Region Name" like '{city}' and "Date" >= to_date('01/01/1900','mm/dd/yyyy')
                        group by "Region Name", "Date"
                        order by "Region Name", "Date"
                        ;"""
                        location = city
                flag = True
                result = pd.read_sql_query(query,conn)
                
                # Creates new dataframe and add location column
                dfList.append(pd.DataFrame({
        "Price ($)":  result.loc[:, "Value"],
        "Date": result.loc[:, "Date"],
        "Location": location
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


with tab2:

        with st.form("my form"):
                state = pd.read_sql_query("""select substr(trim("Region Name"),len(trim("Region Name"))-2,3) as STATE
                        from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
                        group by substr(trim("Region Name"),len(trim("Region Name"))-2,3)
                        order by substr(trim("Region Name"),len(trim("Region Name"))-2,3)
                        limit 51
                        ;""", conn)
                option = st.selectbox('which state would you like to see:', state, key=123)
                submitted = st.form_submit_button("submit")
                if submitted:
                        st.write("You select:", option)
                        df = pd.read_sql_query(f"""select "Region Name" as STATE, year("Date") as YEAR, round(avg("Value"),4) as AVG_HOUSE_PRICE
                        from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
                        where substr(trim("Region Name"),len(trim("Region Name"))-2,3) like '%{option}'
                        group by "Region Name", year("Date")
                        order by "Region Name", year("Date")
                        ;""", conn)
        with st.form("my form2"):
                df = pd.read_sql_query(f"""select "Region Name" as STATE, year("Date") as YEAR, round(avg("Value"),4) as AVG_HOUSE_PRICE
                        from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
                        where substr(trim("Region Name"),len(trim("Region Name"))-2,3) like '%{option}'
                        group by "Region Name", year("Date")
                        order by "Region Name", year("Date")
                        ;""", conn)
                city = df['STATE'].drop_duplicates()
                option2 = st.selectbox('which city would you like to see:', city, key=124)
                submitted = st.form_submit_button("submit")
                if submitted:
                        st.write("You select:", option2)
                        df2 = pd.read_sql_query(f"""select year("Date") as YEAR, round(avg("Value"),4) as AVG_HOUSE_PRICE
                                from "REALESTATE_DATA_ATLAS"."REALESTATE"."ZRHVI2020JUL"
                                where trim("Region Name") like '%{option2}%'
                                group by year("Date")
                                order by year("Date")
                                ;""", conn)
                        plt.style.use("dark_background")
                        fig = plt.figure()
                        plt.plot(df2['YEAR'], df2['AVG_HOUSE_PRICE'])
                        st.pyplot(fig)

