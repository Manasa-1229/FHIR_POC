import requests
import os
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import storage
from dotenv import load_dotenv
load_dotenv()


# Path to your service account key file
service_account_file = os.environ.get('SERVICE_ACCOUNT_FILE')

# Load credentials explicitly
credentials = service_account.Credentials.from_service_account_file(service_account_file)
storage_client = storage.Client(credentials=credentials)


# Constants
FHIR_SERVER_BASE_URL = 'https://hapi.fhir.org/baseR4'
GCS_BUCKET_NAME = 'fhir001'
GCS_PATIENT_FILE = 'patients_data.csv'
GCS_ENCOUNTER_FILE = 'encounters_data.csv'
GCS_OBSERVATION_FILE = 'observations_data.csv'
GCS_CONDITION_FILE = 'conditions_data.csv'
RESOURCE_LIMIT = 100  # Adjust based on server capacity

# Google Cloud Storage Client
storage_client = storage.Client()

# Function to upload DataFrame to GCS
def upload_to_gcs(df, gcs_file_name):
    bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_file_name)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    print(f"File {gcs_file_name} uploaded to {GCS_BUCKET_NAME}.")

# Function to fetch FHIR data from a specific resource URL with pagination
def fetch_fhir_data(resource_type):
    url = f"{FHIR_SERVER_BASE_URL}/{resource_type}?_format=json&_count={RESOURCE_LIMIT}"
    headers = {"Accept": "application/fhir+json"}
    resources = []

    while url:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            bundle = response.json()
            resources.extend(bundle.get('entry', []))
            
            # Check for next link in pagination
            url = None
            for link in bundle.get('link', []):
                if link.get('relation') == 'next':
                    url = link['url']
                    break
        else:
            print(f"Failed to fetch {resource_type}, status code: {response.status_code}")
            break

    return resources

# Function to transform patient data
def transform_patient_data(patients):
    patient_data = []
    for entry in patients:
        patient = entry['resource']
        patient_dict = {
            'id': patient.get('id'),
            'family_name': patient.get('name', [{}])[0].get('family', None),
            'given_name': ' '.join(patient.get('name', [{}])[0].get('given', [])),
            'gender': patient.get('gender', None),
            'birthDate': patient.get('birthDate', None),
            'address': patient.get('address', [{}])[0].get('line', [None])[0],
            'city': patient.get('address', [{}])[0].get('city', None),
            'state': patient.get('address', [{}])[0].get('state', None),
            'country': patient.get('address', [{}])[0].get('country', None),
        }
        patient_data.append(patient_dict)
    return pd.DataFrame(patient_data)

# Function to transform encounter data
def transform_encounter_data(encounters):
    encounter_data = []
    for entry in encounters:
        encounter = entry['resource']
        encounter_dict = {
            'id': encounter.get('id'),
            'patient_id': encounter.get('subject', {}).get('reference', '').split('/')[-1],
            'start': encounter.get('period', {}).get('start'),
            'end': encounter.get('period', {}).get('end'),
            'location': ', '.join([loc.get('location', {}).get('display', '') for loc in encounter.get('location', [])])
        }
        encounter_data.append(encounter_dict)
    return pd.DataFrame(encounter_data)

# Function to transform observation data
def transform_observation_data(observations):
    observation_data = []
    for entry in observations:
        observation = entry['resource']
        observation_dict = {
            'id': observation.get('id'),
            'status': observation.get('status'),
            'code': observation.get('code', {}).get('text'),
            'value': observation.get('valueQuantity', {}).get('value'),
            'unit': observation.get('valueQuantity', {}).get('unit'),
            'effective_start': observation.get('effectivePeriod', {}).get('start'),
            'effective_end': observation.get('effectivePeriod', {}).get('end'),
        }
        observation_data.append(observation_dict)
    return pd.DataFrame(observation_data)

# Function to transform condition data
def transform_condition_data(conditions):
    condition_data = []
    for entry in conditions:
        condition = entry['resource']
        condition_dict = {
            'id': condition.get('id'),
            'patient_id': condition.get('subject', {}).get('reference', '').split('/')[-1],
            'code': condition.get('code', {}).get('text'),
            'clinical_status': condition.get('clinicalStatus', {}).get('text'),
            'verification_status': condition.get('verificationStatus', {}).get('text'),
            'onset_date': condition.get('onsetDateTime')
        }
        condition_data.append(condition_dict)
    return pd.DataFrame(condition_data)

# Main script to fetch data from FHIR server, transform, and upload to GCS
def main():
    # Fetch data from FHIR server
    patients = fetch_fhir_data("Patient")
    encounters = fetch_fhir_data("Encounter")
    observations = fetch_fhir_data("Observation")
    conditions = fetch_fhir_data("Condition")

    # Transform data into DataFrames
    df_patients = transform_patient_data(patients)
    df_encounters = transform_encounter_data(encounters)
    df_observations = transform_observation_data(observations)
    df_conditions = transform_condition_data(conditions)

    # Upload transformed data to GCS
    upload_to_gcs(df_patients, GCS_PATIENT_FILE)
    upload_to_gcs(df_encounters, GCS_ENCOUNTER_FILE)
    upload_to_gcs(df_observations, GCS_OBSERVATION_FILE)
    upload_to_gcs(df_conditions, GCS_CONDITION_FILE)

if __name__ == "__main__":
    main()
