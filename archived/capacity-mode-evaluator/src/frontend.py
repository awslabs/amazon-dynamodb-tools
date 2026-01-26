import streamlit as st
import pandas as pd
import glob
import os
from datetime import datetime

# Dynamically locate the latest CSV file in ./output/
def get_latest_csv():
    csv_files = glob.glob("./output/analysis_summary*.csv")
    if not csv_files:
        st.error("No CSV files found in the output directory.")
        st.stop()
    return max(csv_files, key=os.path.getctime)

file_path = get_latest_csv()
df = pd.read_csv(file_path)

st.set_page_config(layout="wide")  # Use full screen width
st.title("DynamoDB Cost & Scaling Analysis")

# Add a toggle button to filter between Optimized and Not Optimized tables
status_filter = st.radio("Filter Tables by Status:", ("All", "Optimized", "Not Optimized"))
if status_filter != "All":
    df = df[df['status'] == status_filter]

# Group tables and their indexes
table_groups = df.groupby('base_table_name')

# Summary statistics
st.subheader("Summary Statistics")
col1, col2, col3 = st.columns(3)
col1.metric(label="Total Current Cost", value=f"${df['current_cost'].sum():,.2f}")
col2.metric(label="Total Recommended Cost", value=f"${df['recommended_cost'].sum():,.2f}")
col3.metric(label="Average Savings %", value=f"{df['savings_pct'].mean():.2f}%")

# Display recommendations in a component style layout
st.subheader("Table Recommendations")
for table, data in table_groups:
    highlight = "🔴" if data.iloc[0]['status'] == "Not Optimized" else "🟢"
    with st.expander(f"{highlight} Table: {table}"):
        st.write(f"**Status:** {data.iloc[0]['status']}")
        st.write(f"**Current Mode:** {data.iloc[0]['current_mode']}")
        st.write(f"**Recommended Mode:** {data.iloc[0]['recommended_mode']}")
        st.write(f"**Current Cost:** ${data.iloc[0]['current_cost']:,.2f}")
        st.write(f"**Recommended Cost:** ${data.iloc[0]['recommended_cost']:,.2f}")
        st.write(f"**Savings %:** {data.iloc[0]['savings_pct']:.2f}%")
        st.write(f"**Autoscaling Enabled:** {data.iloc[0]['autoscaling_enabled']}")
        
        indexes = data.dropna(subset=['index_name'])
        if not indexes.empty:
            st.subheader("Global Secondary Indexes")
            index_list = indexes.to_dict(orient='records')
            
            cols = st.columns(3)  # Create 3 columns for stacking indexes
            for i, index in enumerate(index_list):
                with cols[i % 3]:
                    index_highlight = "🔴" if index['status'] == "Not Optimized" else "🟢"
                    st.markdown(f"{index_highlight} **{index['index_name']}**")
                    st.write(f"**Status:** {index['status']}")
                    st.write(f"**Current Mode:** {index['current_mode']}")
                    st.write(f"**Recommended Mode:** {index['recommended_mode']}")
                    st.write(f"**Current Cost:** ${index['current_cost']:,.2f}")
                    st.write(f"**Recommended Cost:** ${index['recommended_cost']:,.2f}")
                    st.write(f"**Savings %:** {index['savings_pct']:.2f}%")
                    st.write(f"**Autoscaling Enabled:** {index['autoscaling_enabled']}")