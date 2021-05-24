import argparse
import os

from dotenv import load_dotenv

load_dotenv()

# Service account key path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GCP_CREDENTIALS')
INPUT_SUBSCRIPTION = os.getenv('GCP_PUBSUB_SUBSCRIPTION') # projects/your_gcp_project/subscriptions/your_pub_sub_subscription
BIGQUERY_TABLE = os.getenv('GCP_TABLE') # your_project:your_dataset.your_table
BIGQUERY_SCHEMA = os.getenv('GCP_BIGQUERY_SCHEMA') # field_1:FIELD_1_TYPE,field_2:FIELD_2_TYPE,....

def run():
    print('df-test')

    
if __name__ == "__main__":
    run()