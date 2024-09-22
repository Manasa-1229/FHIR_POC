import requests
import pandas as pd
from google.cloud import storage
import os

# Constants
FHIR_SERVER_URL = "https://hapi.fhir.org/baseR4/"
GCS_BUCKET_NAME = "fhir001"
GCS_PATIENT_FILE = "patients.csv"
GCS_ENCOUNTER_FILE = "encounters.csv"
GCS_CONDITION_FILE = "conditions.csv"
GCS_OBSERVATION_FILE = "observations.csv"

# Google Cloud Storage Client
storage_client = storage.Client()

# Function to fetch FHIR resources
def fetch_fhir_resources(resource_type, limit=100):
    url = f"{FHIR_SERVER_URL}/{resource_type}?_count={limit}"
    headers = {"Accept": "application/fhir+json"}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get('entry', [])  # Returns list of resources
    else:
        print(f"Failed to fetch {resource_type}, status code: {response.status_code}")
        return []

# Function to transform Patient resources into a DataFrame
def transform_patient_data(patients):
    patient_data = []
    for entry in patients:
        patient = entry['resource']
        patient_data.append({
            'id': patient['id'],
            'name': patient['name'][0]['family'] if 'name' in patient else None,
            'given': patient['name'][0]['given'][0] if 'name' in patient else None,
            'gender': patient.get('gender'),
            'birthDate': patient.get('birthDate')
        })
    return pd.DataFrame(patient_data)

# Function to transform Encounter resources into a DataFrame
def transform_encounter_data(encounters):
    encounter_data = []
    for entry in encounters:
        encounter = entry['resource']
        encounter_data.append({
            'id': encounter['id'],
            'patient_id': encounter['subject']['reference'].split('/')[-1],
            'status': encounter.get('status'),
            'class': encounter['class']['code'] if 'class' in encounter else None,
            'type': encounter['type'][0]['coding'][0]['display'] if 'type' in encounter else None,
            'start': encounter.get('period', {}).get('start'),
            'end': encounter.get('period', {}).get('end')
        })
    return pd.DataFrame(encounter_data)

# Function to transform Condition resources into a DataFrame
def transform_condition_data(conditions):
    condition_data = []
    for entry in conditions:
        condition = entry['resource']
        condition_data.append({
            'id': condition['id'],
            'patient_id': condition['subject']['reference'].split('/')[-1],
            'clinicalStatus': condition['clinicalStatus']['coding'][0]['code'] if 'clinicalStatus' in condition else None,
            'verificationStatus': condition['verificationStatus']['coding'][0]['code'] if 'verificationStatus' in condition else None,
            'code': condition['code']['coding'][0]['display'] if 'code' in condition else None,
            'onsetDateTime': condition.get('onsetDateTime')
        })
    return pd.DataFrame(condition_data)

# Function to transform Observation resources into a DataFrame
def transform_observation_data(observations):
    observation_data = []
    for entry in observations:
        observation = entry['resource']
        observation_data.append({
            'id': observation['id'],
            'patient_id': observation['subject']['reference'].split('/')[-1],
            'code': observation['code']['coding'][0]['display'] if 'code' in observation else None,
            'value': observation['valueQuantity']['value'] if 'valueQuantity' in observation else None,
            'unit': observation['valueQuantity']['unit'] if 'valueQuantity' in observation else None,
            'effectiveDateTime': observation.get('effectiveDateTime')
        })
    return pd.DataFrame(observation_data)

# Function to save DataFrame to CSV and upload to GCS
def upload_to_gcs(df, file_name):
    df.to_csv(file_name, index=False)
    bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)
    print(f"File {file_name} uploaded to {GCS_BUCKET_NAME}.")

# Step 1: Fetch and Transform Data
patients = fetch_fhir_resources("Patient")
encounters = fetch_fhir_resources("Encounter")
conditions = fetch_fhir_resources("Condition")
observations = fetch_fhir_resources("Observation")

df_patients = transform_patient_data(patients)
df_encounters = transform_encounter_data(encounters)
df_conditions = transform_condition_data(conditions)
df_observations = transform_observation_data(observations)

# Step 2: Save and Upload to GCS
upload_to_gcs(df_patients, GCS_PATIENT_FILE)
upload_to_gcs(df_encounters, GCS_ENCOUNTER_FILE)
upload_to_gcs(df_conditions, GCS_CONDITION_FILE)
upload_to_gcs(df_observations, GCS_OBSERVATION_FILE)

print("Data ingestion completed successfully.")
