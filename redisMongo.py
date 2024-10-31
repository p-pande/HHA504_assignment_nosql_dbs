import pandas as pd
from pymongo import MongoClient
import redis
import json


df = pd.read_csv('dataset.csv')

data = df.to_dict(orient='records')

# Connect to MongoDB

client = MongoClient("mongodb+srv://prabhakarpandey:pandey@prabhakar-test.haqgp.mongodb.net/")
db = client ['test_db']
collection = db['patients']

collection.insert_many(data)

# Connect to Redis
r = redis.StrictRedis(
    host='redis-10870.c263.us-east-1-2.ec2.redns.redis-cloud.com',
    port= 10870,
    password='8Rud68bfY89eKwSocnB9OqzokECAoh9V',
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