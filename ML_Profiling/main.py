from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler

from randomGenerator import insert_driver_ratings, insert_user_ratings
from driver import work_flow, app

# work_flow()

@asynccontextmanager
async def lifespan(app:FastAPI):
    scheduler = BackgroundScheduler()
    scheduler.add_job(work_flow, "interval", minutes=360)
    scheduler.start()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def test():
    return "Running"