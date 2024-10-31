import pandas as pd
from pymongo import MongoClient
import redis
import json
import os

url = 'https://raw.githubusercontent.com/hantswilliams/HHA-504-2024/refs/heads/main/other/module8/module8_nosql_hw.csv'
df = pd.read_csv(url)

data = df.to_dict(orient='records')

# Connect to MongoDB

client = MongoClient(os.getenv("MONGO"))
db = client ['test_db']
collection = db['patients']

collection.insert_many(data)

# Connect to Redis
r = redis.StrictRedis(
    host= os.getenv("REDISHOST"),
    port= 10870,
    password= os.getenv("REDISPASS"),
    decode_responses=True
)

for _, row in df.iterrows():
    patient_data = row.to_dict()
    r.set(patient_data['PatientID'], json.dumps(patient_data))
    
    
# Retrieve data for PatientID=1
patient_id = '1'
patient_data = json.loads(r.get(patient_id))
print("Patient Data:", patient_data)


# Update TreatmentPlan
patient_data['TreatmentPlan'] = 'Updated Plan for PT'

# Save updated data back to Redis
r.set(patient_id, json.dumps(patient_data))

# Verify the update
updated_data = json.loads(r.get(patient_id))
print("Updated Patient Data:", updated_data)

patient_ids = r.keys('*')
print("All Patient IDs:", patient_ids)