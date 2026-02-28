import streamlit as st
import pandas as pd
import time

st.set_page_config(page_title="Swasthya-Matrix Dashboard", page_icon="🏥", layout="wide")

CLUSTER_MAP = {0: "Safe", 1: "Critical", 2: "Average"}

st.title("🏥 Swasthya-Matrix: Real-Time Health Resource Allocation")
st.markdown("Live monitoring of state-wise health metrics, cluster predictions, and vulnerability indices.")

placeholder = st.empty()

while True:
    try:
        df = pd.read_csv("output_sink/latest_predictions.csv")
        
        df['Current_Cluster'] = df['KMeans_Cluster'].map(CLUSTER_MAP)
        df['Predicted_Severity'] = df['RF_Severity_Prediction'].map(CLUSTER_MAP)
        
        critical_states = df[df['Current_Cluster'] == 'Critical']['State'].tolist()
        
        with placeholder.container():
            st.caption(f"Last updated: {df['Month_Ending'].iloc[0]}")
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total States Monitored", len(df))
            col2.metric("Critical States (Emergency)", len(critical_states))
            col3.metric("Max Vulnerability Score", f"{df['Vulnerability_Score'].max():.2f}")
            
            if critical_states:
                st.error(f"🚨 **EMERGENCY ALLOCATION REQUIRED:** The following states have entered the Critical cluster: {', '.join(critical_states)}")
            else:
                st.success("✅ **STABLE:** All states are currently operating within Safe or Average parameters.")
            
            st.markdown("---")
            
            chart_col, table_col = st.columns([1, 1])
            
            with chart_col:
                st.subheader("State Vulnerability Scores")
                st.write("Score from 1 (Safest) to 100 (Most Critical)")
                st.bar_chart(df.set_index('State')['Vulnerability_Score'])
                
            with table_col:
                st.subheader("Live Model Predictions Feed")
                display_df = df[['State', 'Current_Cluster', 'Predicted_Severity', 'Vulnerability_Score']]
                st.dataframe(display_df, width='stretch', hide_index=True)

    except FileNotFoundError:
        with placeholder.container():
            st.warning("⏳ Waiting for data stream... Please ensure `mock_data_generator.py` is running.")
            
    time.sleep(5)