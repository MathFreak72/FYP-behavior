# Command to run

```
> uvicorn feedback_api:app --reload
```

POST request structure:
Driver: http://127.0.0.1:8000/driver?safety_rating=4&cleanliness_rating=5&punctuality=4&cancelled_ride=0&overall_rating=5
User: http://127.0.0.1:8000/user?trust_rating=4&cleanliness_rating=5&punctuality=4&cancelled_ride=0&overall_rating=5
