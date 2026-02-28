import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

os.makedirs("data", exist_ok=True)

states = ["Bihar", "Maharashtra", "Kerala", "Uttar Pradesh", "Tamil Nadu"]

start_date = datetime.now()
data = []

for i in range(100):
    current_date = start_date + timedelta(days=i)
    for state in states:
        data.append({
            "Date": current_date.strftime("%Y-%m-%d"),
            "State": state,
            "Region_Index": float(np.random.choice([0.0, 1.0])),
            "ANC_Registered": float(np.random.randint(50, 5000)),
            "Institutional_Delivery": float(np.random.randint(40, 4800)),
            "C_Section": float(np.random.randint(10, 1000)),
            "Child_Diarrhoea": float(np.random.randint(0, 500)),
            "Child_TB": float(np.random.randint(0, 50)),
            "Immunization_MR": float(np.random.randint(50, 5000)),
            "Low_Birth_Weight": float(np.random.randint(5, 500)),
            "Infant_Death_Sepsis": float(np.random.randint(0, 20)),
            "Maternal_Death_Bleeding": float(np.random.randint(0, 10)),
            "Adult_Death_TB": float(np.random.randint(0, 30)),
            "Adult_Suicide": float(np.random.randint(0, 15)),
            "PW_Hypertension": float(np.random.randint(10, 300)),
            "Child_Malaria": float(np.random.randint(0, 200)),
            "Severe_Anaemia_Treated": float(np.random.randint(5, 400)),
            "Condoms_Distributed": float(np.random.randint(100, 10000)),
            "Early_Breastfeeding": float(np.random.randint(40, 4800))
        })

df = pd.DataFrame(data)
df.to_csv("data/synthetic_kafka_stream.csv", index=False)
print("Synthetic State-wise daily data generated successfully at data/synthetic_kafka_stream.csv")