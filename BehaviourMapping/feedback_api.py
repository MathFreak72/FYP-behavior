import json

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from insertFeedack import driver_feedback, user_feedback

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# time = datetime.now(pytz.utc)
cloud_config= {
    'secure_connect_bundle': 'secure-connect-fyp-driver.zip'
}

with open("fyp-driver-token.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
    print(row[0])
else:
    print("An error occurred.")

driver_feedback(app, session)
user_feedback(app, session)