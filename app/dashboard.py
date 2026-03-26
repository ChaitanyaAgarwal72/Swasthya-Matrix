import streamlit as st
import pandas as pd
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from llm_insights import get_gemini_insight

st.set_page_config(page_title="Swasthya-Matrix Dashboard", page_icon="🏥", layout="wide")

CLUSTER_MAP = {0: "Low Risk", 1: "Moderate Risk", 2: "High Risk"}

st.title("🏥 Swasthya-Matrix: Real-Time Predictive Health Surveillance System")
st.markdown("Live monitoring of state-wise health metrics, cluster predictions, and vulnerability indices.")

placeholder = st.empty()

while True:
    try:
        df = pd.read_csv("output_sink/latest_predictions.csv")
        
        if 'Severity_Level' not in df.columns:
            raise FileNotFoundError
        
        df['Severity'] = df['Severity_Level'].map(CLUSTER_MAP)
        
        critical_locations = df[df['Severity'] == 'High Risk']['Location'].tolist()
        
        with placeholder.container():
            st.caption(f"Last updated: {df['Month_Ending'].max()}")
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total States Monitored", df['State'].nunique())
            col2.metric("High Risk Regions", len(critical_locations))
            col3.metric("Max Vulnerability Score", f"{df['Vulnerability_Score'].max():.2f}")
            
            if critical_locations:
                st.error(f"🚨 **HIGH RISK ALERT:** The following regions are classified as High Risk: {', '.join(critical_locations)}")
            else:
                st.success("✅ **STABLE:** All regions are currently operating within Low or Moderate Risk parameters.")
            
            st.markdown("---")
            
            chart_col, table_col = st.columns([1, 1])
            
            with chart_col:
                st.subheader("Regional Vulnerability Scores")
                st.write("Score from 1 (Safest) to 100 (Most Vulnerable)")
                st.bar_chart(df.set_index('Location')['Vulnerability_Score'])
                
            with table_col:
                st.subheader("Live Model Predictions Feed")
                display_df = df[['Location', 'Severity', 'Vulnerability_Score']]
                def color_severity(val):
                    if val == 'High Risk':
                        return 'background-color: #ff4b4b; color: white'
                    elif val == 'Moderate Risk':
                        return 'background-color: #ffa500; color: white'
                    return 'background-color: #21c354; color: white'
                styled_df = display_df.style.map(color_severity, subset=['Severity'])
                st.dataframe(styled_df, width='stretch', hide_index=True)

            st.markdown("---")
            st.subheader("Advanced Risk Analytics")
            
            viz_col1, viz_col2 = st.columns(2)
            
            with viz_col1:
                st.markdown("##### 📊 Risk Level Distribution")
                severity_counts = df['Severity'].value_counts()
                st.bar_chart(severity_counts)
                
            with viz_col2:
                st.markdown("##### ⚠️ Top 10 Most Vulnerable Regions")
                top_10_df = df.nlargest(10, 'Vulnerability_Score').set_index('Location')
                st.bar_chart(top_10_df['Vulnerability_Score'])

            high_risk_df = df[df['Severity'] == 'High Risk']
            if not high_risk_df.empty:
                st.markdown("---")
                st.subheader("🤖 GenAI Emergency Insights")
                
                try:
                    source_df = pd.read_csv("data/cleaned_data_hmis.csv")
                    
                    metric_cols = ['ANC_Registered', 'Institutional_Delivery', 'C_Section', 'Child_Diarrhoea', 
                                   'Child_TB', 'Immunization_MR', 'Low_Birth_Weight', 'Infant_Death_Sepsis', 
                                   'Maternal_Death_Bleeding', 'Adult_Death_TB', 'Adult_Suicide', 'PW_Hypertension', 
                                   'Child_Malaria', 'Severe_Anaemia_Treated', 'Condoms_Distributed', 'Early_Breastfeeding']
                    
                    for index, row in high_risk_df.iterrows():
                        loc = row['Location']
                        score = round(row['Vulnerability_Score'], 2)
                        current_date = row['Month_Ending']
                        state = row['State']
                        region_type = row.get('Region_Type', 'Rural')
                        
                        source_row = source_df[(source_df['State'] == state) & 
                                              (source_df['Region_Type'] == region_type)].iloc[-1] if len(source_df[(source_df['State'] == state) & (source_df['Region_Type'] == region_type)]) > 0 else None
                        
                        if source_row is not None:
                            available_cols = [col for col in metric_cols if col in source_row.index]
                            metrics_dict = {col: source_row[col] for col in available_cols}
                            metrics_text = "\n".join([f"- {k}: {v}" for k, v in metrics_dict.items()])
                        else:
                            metrics_text = "Data not available for this region."
                        
                        insight = get_gemini_insight(loc, current_date, score, metrics_text)
                        st.info(f"**{current_date} | {loc} (Score: {score})**\n\n{insight}")
                
                except FileNotFoundError:
                    st.warning("⚠️ Source data not available for health metrics.")

    except FileNotFoundError:
        with placeholder.container():
            st.warning("⏳ Waiting for data stream... Kafka producer and Spark consumer are starting up.")
            
    time.sleep(1)