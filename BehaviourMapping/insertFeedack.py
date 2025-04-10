from fastapi import HTTPException

def driver_feedback(app, session):
    @app.post("/driver")
    async def user(safety_rating: int, cleanliness_rating: int, punctuality: int, cancelled_ride: int, overall_rating: int):
        try:
            insert_query = """INSERT INTO driverdb.drivers(driver_id, created_at, safety_rating, cleanliness_rating, punctuality, cancelled_ride, overall_rating) 
                              VALUES(uuid(), toTimeStamp(now()), %s, %s, %s, %s, %s)"""
            session.execute(insert_query,
                                   (safety_rating, cleanliness_rating, punctuality, cancelled_ride, overall_rating))
            return {"Message": "Driver's feedback successfully entered."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

def user_feedback(app, session):
    @app.post("/user")
    async def user(trust_rating: int, cleanliness_rating: int, punctuality: int, cancelled_ride: int, overall_rating: int):
        try:
            insert_query = """INSERT INTO driverdb.users(user_id, created_at, trust_rating, cleanliness_rating, punctuality, cancelled_ride, overall_rating) 
                              VALUES(uuid(), toTimeStamp(now()), %s, %s, %s, %s, %s)"""
            session.execute(insert_query,
                                   (trust_rating, cleanliness_rating, punctuality, cancelled_ride, overall_rating))
            return {"Message": "User's feedback successfully entered."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
