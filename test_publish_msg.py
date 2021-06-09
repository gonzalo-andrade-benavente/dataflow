# requirements
# google-cloud-pubsub==1.7.0 
import json
import time
from datetime import datetime
from random import random
from google.auth import jwt
from google.cloud import pubsub_v1
import uuid

# --- Base variables and auth path
CREDENTIALS_PATH = "credentials/xenon-chain-308319-c511b4245b0d.json"
PROJECT_ID = "xenon-chain-308319"
TOPIC_ID = "inbound-topic"
MAX_MESSAGES = 1

# --- PubSub Utils Classes
class PubSubPublisher:
    def __init__(self, credentials_path, project_id, topic_id):
        credentials = jwt.Credentials.from_service_account_info(
            json.load(open(credentials_path)),
            audience="https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        )
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

    def publish(self, data: str):
        result = self.publisher.publish(
            self.topic_path, 
            data.encode("utf-8"),
            eventId=str(uuid.uuid4()), #"f920b391-b42d-11eb-aa2c-a7bf513d27f9",
            timestamp="1620939511671",
            datetime="2021-05-13 16:58:31",
            country="CL",
            eventType="sales",
            entityType="salesCreation",
            channel="pubsub"
            )
        #result = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        return result


# --- Main publishing script
def main():
    i = 0
    publisher = PubSubPublisher(CREDENTIALS_PATH, PROJECT_ID, TOPIC_ID)
    sales = open('data-example2/sales1.json')
    #print(sales)
    while i < MAX_MESSAGES:
        data = json.loads( sales.read().replace("[", "").replace("]","") )
        publisher.publish(json.dumps(data))
        time.sleep(random())
        i += 1

if __name__ == "__main__":
    main()