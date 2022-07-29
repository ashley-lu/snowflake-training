import pandas as pd
import streamlit as st
import snowflake.connector as sf
from secret import secret
import csv

conn = sf.connect(
        account=secret.get('account'),
        user=secret.get('user'),
        password=secret.get('password'),
        database='ECONOMY_DATA_ATLAS',
        schema='ECONOMY',
        warehouse='COMPUTE_WH')