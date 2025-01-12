from fastapi import FastAPI
from pydantic import BaseModel
from faker import Faker
import random
from datetime import datetime, timedelta

# Setup FastAPI
app = FastAPI()

# Initialize Faker instance
fake = Faker()

# Pydantic model to represent the structure of the event
class BehaviorEvent(BaseModel):
    ip_address: str
    page_url: str
    time_spent: float  # Time spent on the page in seconds

# Function to generate a random event for a user
def generate_fake_event():
    ip_address = fake.ipv4()  # Generate a fake IP address
    page_url = fake.uri_path()  # Generate a random URL path
    time_spent = round(random.uniform(30.0, 300.0), 2)  # Random time between 30s to 5 minutes
    event = BehaviorEvent(
        ip_address=ip_address,
        page_url=page_url,
        time_spent=time_spent
    )
    return event

# Function to generate events for a specific day
def generate_events_for_day(date: datetime):
    events_for_day = []
    # Random number of users (between 5 and 15 users per day)
    num_users = random.randint(5, 15)
    
    for _ in range(num_users):
        ip_address = fake.ipv4()  # Random IP for the user
        # Random number of events per user (between 1 and 5 events per user)
        num_events = random.randint(1, 5)
        
        user_events = []
        for _ in range(num_events):
            event = generate_fake_event()  # Generate a fake event
            user_events.append(event.dict())  # Append the event as a dictionary
            
        events_for_day.append({
            "date": date.strftime("%Y-%m-%d"),
            # "ip_address": ip_address,
            "user_events": user_events
        })
    
    return events_for_day

# Endpoint to generate events for the last 30 days
@app.get("/generate-events")
def generate_events():
    """
    Generate events for the last 30 days, with random users and events per user.
    """
    all_events = []
    today = datetime.now()

    # Generate events for the last 30 days
    for day_offset in range(30):
        date = today - timedelta(days=day_offset)
        daily_events = generate_events_for_day(date)
        all_events.extend(daily_events)
    
    return all_events

