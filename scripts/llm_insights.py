import google.genai as genai
import streamlit as st
import os
import dotenv

dotenv.load_dotenv()

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

@st.cache_data(ttl=3600)
def get_gemini_insight(location, date, vulnerability_score, metrics_text):
    prompt = f"""Act as a public health expert. The region '{location}' on {date} has been flagged as High Risk with a severe vulnerability score of {vulnerability_score}/100. 
    
    Here are the exact health metrics recorded for this region that triggered this alarm right now:
    {metrics_text}
    
    Analyze these specific numbers. Provide a strict, 2-sentence actionable emergency response recommendation pinpointing the exact cause."""
    
    try:
        response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt
    )
        return response.text
    except Exception as e:
        return "AI Insight temporarily unavailable."