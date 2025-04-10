import uuid
import random
import pytz

from cassandra.cluster import Cluster
from datetime import datetime, timezone

def create_driver_db(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS driverdb.drivers (
            driver_id UUID,
            created_at TIMESTAMP,
            safety_rating INT,
            cleanliness_rating INT,
            punctuality INT,
            cancelled_ride INT,
            overall_rating INT,
            PRIMARY KEY (driver_id, created_at)
        );
    """)

    return session

def create_user_db(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS driverdb.users (
            user_id UUID,
            created_at TIMESTAMP,
            trust_rating INT,
            cleanliness_rating INT,
            punctuality INT,
            cancelled_ride INT,
            overall_rating INT,
            PRIMARY KEY (user_id, created_at)
        );
    """)

    return session

def fetch_old_records_drivers(time, session):
    # global time
    # Fetching filtered records (Only entries where created_at < current time)
    rows = session.execute("SELECT * FROM driverdb.drivers;")  # Fetch all records

    # Filter records where created_at < current_time
    filtered_rows = [
        (row.driver_id, row.safety_rating, row.cleanliness_rating, row.punctuality, row.cancelled_ride, row.overall_rating)
        for row in rows if row.created_at.replace(tzinfo=timezone.utc) < time
    ]
    
    return filtered_rows


def fetch_old_records_users(time, session):
    # global time
    # Fetching filtered records (Only entries where created_at < current time)
    rows = session.execute("SELECT * FROM driverdb.users;")  # Fetch all records

    # Filter records where created_at < current_time
    filtered_rows = [
        (row.user_id, row.trust_rating, row.cleanliness_rating, row.punctuality, row.cancelled_ride,
         row.overall_rating)
        for row in rows if row.created_at.replace(tzinfo=timezone.utc) < time
    ]

    return filtered_rows

def write_to_txt_file_driver(records, filename="driver_output.txt"):
    # Writing data to a text file in the required format
    with open(filename, "w") as file:
        for row in records:
            file.write(",".join(map(str, row)) + "\n")  
    
    print(f"Filtered data saved to {filename}")

def write_to_txt_file_user(records, filename="user_output.txt"):
    # Writing data to a text file in the required format
    with open(filename, "w") as file:
        for row in records:
            file.write(",".join(map(str, row)) + "\n")

    print(f"Filtered data saved to {filename}")

def delete_old_driver_ratings(time, session):
    # global time
    get_driver_ids_query = "SELECT DISTINCT driver_id FROM driverdb.drivers"
    rows = session.execute(get_driver_ids_query)
    driver_ids = [row.driver_id for row in rows]

    for driver_id in driver_ids:
        # Fetch all records with created_at < now() for this driver
        select_query = """
            SELECT driver_id, created_at FROM driverdb.drivers 
            WHERE driver_id = %s AND created_at < %s ALLOW FILTERING
        """
        old_records = session.execute(select_query, (driver_id, time))
        
        for record in old_records:
            delete_query = "DELETE FROM driverdb.drivers WHERE driver_id = %s AND created_at = %s"
            session.execute(delete_query, (record.driver_id, record.created_at))
    print("Driver records deleted")

def delete_old_user_ratings(time, session):
    # global time
    get_user_ids_query = "SELECT DISTINCT user_id FROM driverdb.users"
    rows = session.execute(get_user_ids_query)
    user_ids = [row.user_id for row in rows]

    for user_id in user_ids:
        # Fetch all records with created_at < now() for this driver
        select_query = """
            SELECT user_id, created_at FROM driverdb.users 
            WHERE user_id = %s AND created_at < %s ALLOW FILTERING
        """
        old_records = session.execute(select_query, (user_id, time))

        for record in old_records:
            delete_query = "DELETE FROM driverdb.users WHERE user_id = %s AND created_at = %s"
            session.execute(delete_query, (record.user_id, record.created_at))
    print("User records deleted")

def convertToTxtDriver(time, session):
    old_records_driver = fetch_old_records_drivers(time, session)
    if old_records_driver:
        write_to_txt_file_driver(old_records_driver)
        delete_old_driver_ratings(time, session)
    else:
        raise ValueError

def convertToTxtUser(time, session):
    old_records_user = fetch_old_records_users(time, session)
    if old_records_user:
        write_to_txt_file_user(old_records_user)
        delete_old_user_ratings(time, session)
    else:
        raise ValueError