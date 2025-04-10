import json
import sqlite3
import pytz

from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from fastapi import FastAPI

from DB_Functions import create_driver_db, create_user_db
from randomGenerator import insert_driver_ratings, insert_user_ratings
from insertFeedack import driver_feedback, user_feedback
from DB_Functions import convertToTxtDriver, convertToTxtUser

from AverageMapReduce import Mapper, Reducer
from MLProfiling import ML_Profiling

app = FastAPI()

def work_flow():
    time = datetime.now(pytz.utc)
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

    create_driver_db(session)
    # insert_driver_ratings(session)
    # driver_feedback(app, session)
    convertToTxtDriver(time, session)

    create_user_db(session)
    # insert_user_ratings(session)
    # user_feedback(app, session)
    convertToTxtUser(time, session)

    driver_filepath = "driver_output.txt"
    user_filepath = "user_output.txt"

    DB_NAME = "driver.db"
    DRIVER_TABLE_NAME = "driver"
    USER_TABLE_NAME = "user"

    driver_data = Reducer(Mapper(driver_filepath))
    driver_data = ML_Profiling(driver_data, 6)

    user_data = Reducer(Mapper(user_filepath))
    user_data = ML_Profiling(user_data, 6)

    # Connect to SQLite database
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for driver_id, values in driver_data.items():
        cursor.execute(f'SELECT * FROM {DRIVER_TABLE_NAME} WHERE driver_id = ?', (driver_id, ))
        exists = cursor.fetchall()
        print(exists)
        if not exists:
            newValues = [min(5, max(1, 4.5 + val)) for val in values]
            cursor.execute(f"""
            INSERT INTO {DRIVER_TABLE_NAME} (
                driver_id,
                safety_rating,
                cleanliness_rating,
                punctuality,
                cancelled_ride,
                overall_rating
            ) VALUES (?, ?, ?, ?, ?, ?)
            """, (driver_id, *newValues))
        else:
            cursor.execute (f"""
                UPDATE {DRIVER_TABLE_NAME}
                SET safety_rating = MIN(5, MAX(1, safety_rating + ?)),
                    cleanliness_rating = MIN(5, MAX(1, cleanliness_rating + ?)),
                    punctuality = MIN(5, MAX(1, punctuality + ?)),
                    cancelled_ride = MIN(5, MAX(1, cancelled_ride + ?)),
                    overall_rating = MIN(5, MAX(1, overall_rating + ?))
                WHERE driver_id = ?
            """, (*values, driver_id))

    cursor.execute(f"""select * from {DRIVER_TABLE_NAME};""")
    print(cursor.fetchall())

    for user_id, values in user_data.items():
        cursor.execute(f'SELECT * FROM {USER_TABLE_NAME} WHERE user_id = ?', (user_id, ))
        exists = cursor.fetchall()
        print(exists)
        if not exists:
            newValues = [min(5, max(1, 4.5 + val)) for val in values]
            cursor.execute(f"""
            INSERT INTO {USER_TABLE_NAME} (
                user_id,
                trust_rating,
                cleanliness_rating,
                punctuality,
                cancelled_ride,
                overall_rating
            ) VALUES (?, ?, ?, ?, ?, ?)
            """, (user_id, *newValues))
        else:
            cursor.execute (f"""
                UPDATE {USER_TABLE_NAME}
                SET trust_rating = MIN(5, MAX(1, safety_rating + ?)),
                    cleanliness_rating = MIN(5, MAX(1, cleanliness_rating + ?)),
                    punctuality = MIN(5, MAX(1, punctuality + ?)),
                    cancelled_ride = MIN(5, MAX(1, cancelled_ride + ?)),
                    overall_rating = MIN(5, MAX(1, overall_rating + ?))
                WHERE user_id = ?
            """, (*values, user_id))

    cursor.execute(f"""select * from {USER_TABLE_NAME};""")
    print(cursor.fetchall())

    conn.commit()
    conn.close()