from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
# Create or get an existing Spark session
spark = SparkSession.builder \
    .appName('bci') \
    .master('local') \
    .getOrCreate()

average_readings = None
# Function to show available tables
def show_tables():
    return spark.sql("SHOW TABLES").toPandas()

# Function to query the in-memory table
def query_data():
    return spark.sql("SELECT * FROM avg_readings").toPandas()

# Using Streamlit to display the data
import streamlit as st

if 'first_run' not in st.session_state:
    st.session_state['first_run'] = True

if 'global_df' not in st.session_state:
    st.session_state['global_df'] = []

if st.session_state['first_run']:
    df = spark.read\
        .options(header=True,inferSchema='True',delimiter=',')\
        .csv("data/processed_bci_csv/part-00000-dc01e588-8401-4856-b1ba-7b3f342a2119-c000.csv")
    
    st.session_state['data_schema'] = df.schema

    signals = spark\
            .readStream\
            .format('csv')\
            .schema(st.session_state['data_schema'])\
            .option("header",True)\
            .option("maxFilesPerTrigger",1)\
            .load(r"data/processed_bci_csv/")
    
    average_readings = signals.select([F.avg(col).alias('avg_' + col) for col in signals.columns])
    query = average_readings.writeStream\
            .outputMode("complete")\
            .format("memory")\
            .queryName("avg_readings")\
            .trigger(processingTime='1000 milliseconds')\
            .start()
    
    st.session_state['first_run'] = False
    
import pandas as pd
import time
st.title("Real-time Data from PySpark Streaming")
 
# Button to refresh data
def refresh_data():
    df = query_data()
    st.write(df)
    st.session_state['global_df'].append(df)

    if len(st.session_state['global_df'])>100:
        st.session_state['global_df'].pop(0)
    result_df = pd.concat(st.session_state['global_df'])
    result_df['step'] = range(1,len(st.session_state['global_df']))

    for column in df.columns:
        st.text(column)
        st.line_chart(result_df[['step',column]], x="step", y=column, height = 150)

refresh_interval = 1
last_refresh_time = st.session_state.get('last_refresh_time', 0)

if time.time() - last_refresh_time > refresh_interval:
    last_refresh_time = time.time()
    st.session_state['last_refresh_time'] = last_refresh_time
    refresh_data()
    st.rerun()
