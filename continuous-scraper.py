import asyncio
import time
from tqdm import tqdm
import shutil
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.operations import InsertOne
from dotenv import load_dotenv
import os
import httpx
import motor.motor_asyncio

'''



Roblox Continuous Game Scraper
by nouhidev



'''

_VERSION = "0.0.1-continuous"

# ------- [ Scraping Parameters ] -------

# https://games.roblox.com/v1/games/votes?universeIds=
# https://games.roblox.com/v1/games?universeIds=

BASE_URL = "https://games.roblox.com/v1/games?universeIds="

# UIDs/Url (Default: 100)
BATCH_SIZE = 100

# Initial delay between requests
INITIAL_REQUESTS_DELAY = 1

RATE_LIMIT_DELAY_PENALTY_MULTIPLIER = 2

# Number of consecutive requests without rate limiting required to reset the delay between requests
RATE_LIMIT_PENALTY_RESET_THRESHOLD = 10

# UID to stop scraping at (Default: 5060800000)
END_ID = 5060800000

# Amount of seconds that have to be passed before returning the data (Default: 3)
RETURN_DATA_MINIMUM_DELAY = 2

MAX_RETRIES = 5

# Display intensive debug information (WIP)
DISPLAY_DEBUG_INFORMATION = False

# Leave out calculations and unnecessary printing
PERFORMANCE_MODE = True

# -------- [ File Output Paths ] --------

COUNT_FILE_PATH = "start_uid.txt"

ERRORED_BATCHES_FILE_PATH = "errored_batches.txt"

# -------- [ Requests Managing ] --------

# Locally used to keep track of the start of each batch
start_uid = 0

# Active delay between each request
current_requests_delay = 0

# Keeps track of the amount of consecutive requests without rate limiting
consecutive_no_rate_limit = 0

# Keeps track of the unresolved requests
unresolved_requests = 0

# Keeps track of the games added in the current session
games_added_in_session = 0

# ------- [ MongoDB Integration ] -------

load_dotenv()

MONGODB_DATABASE_NAME = "games"

MONGODB_COLLECTION_NAME = "scraped"

bulk_operations = []

uri = os.getenv("DATABASE_URL")

client = motor.motor_asyncio.AsyncIOMotorClient(uri, server_api=ServerApi('1'))

db = client[MONGODB_DATABASE_NAME]
collection = db[MONGODB_COLLECTION_NAME]

# ----- [ Get Start UID from File ] -----

try:
    with open(COUNT_FILE_PATH, "r") as count_file:
        count_str = count_file.read().strip()
        if count_str:
            start_uid = int(count_str)
except FileNotFoundError:
    with open(COUNT_FILE_PATH, "w") as count_file:
        count_file.write(str(start_uid))

# ----- [ ----------------------- ] -----

def print_stats(_current_requests_delay):
    global games_added_in_session
    os.system('cls')
    print("===================================")
    print(f"Ongoing requests: {unresolved_requests}")
    print(f"Games added in session: {games_added_in_session}")
    print(f"Delay between new requests: {_current_requests_delay} seconds")

async def fetch_data(session, batch_start, batch_end, request_id):
    global consecutive_no_rate_limit, current_requests_delay, unresolved_requests, games_added_in_session

    unresolved_requests += 1

    retry_counter = 0

    universe_ids = ",".join(str(i) for i in range(batch_start, batch_end))
    url = BASE_URL + universe_ids
    
    # Decrease the current delay between requests after consecutive successful requests
    if consecutive_no_rate_limit >= RATE_LIMIT_PENALTY_RESET_THRESHOLD:
        current_requests_delay = max(current_requests_delay / RATE_LIMIT_DELAY_PENALTY_MULTIPLIER, INITIAL_REQUESTS_DELAY)

    while retry_counter < MAX_RETRIES:
        # Try to sent the HTTP request
        try:
            response = await session.get(url, timeout=10)
        # HTTP request error handling
        except Exception as e:
            retry_counter += 1
            await asyncio.sleep(current_requests_delay)
            continue

        # Rate limit handling for v1/games and v1/votes endpoints
        if response.status_code == 503 or response.status_code == 429:
            print(f"[_id{request_id}] Batch {batch_start:,}-{batch_end:,} got rate limited")
            # Penalize the script for getting rate limited
            current_requests_delay *= RATE_LIMIT_DELAY_PENALTY_MULTIPLIER
            consecutive_no_rate_limit = 0
            retry_counter += 1
            await asyncio.sleep(current_requests_delay)
            continue
        
        # Try to parse the response as JSON
        try:
            data = response.json()
            consecutive_no_rate_limit += 1

            documents_to_insert = []

            for entry in data["data"]:
                uid = entry["id"]
                place_id = entry["rootPlaceId"]
                visits = entry["visits"]
                name = entry["name"]
                favoritedCount = entry["favoritedCount"]
                
                document = {
                    "uid": uid,
                    "placeId": place_id,
                    "visits": visits,
                    "name": name,
                    "favoritedCount": favoritedCount
                }

                if entry["genre"] == "Horror" and entry["visits"] >= 1000:
                    games_added_in_session += 1
                    documents_to_insert.append(document)
            
            await collection.insert_many(documents_to_insert)
            unresolved_requests -= 1
            return
        # If the parsing fails retry
        except Exception as e:
            print(f"[_id{request_id}] Failed to parse the response as JSON!")
            retry_counter += 1
            await asyncio.sleep(current_requests_delay)
            continue
    
    # All retries have failed
    unresolved_requests -= 1
        
async def main():
    global start_uid

    async with httpx.AsyncClient(http2=True, limits=httpx.Limits(max_connections=None, max_keepalive_connections=0)) as session:
        request_id = 1
        current_requests_delay = INITIAL_REQUESTS_DELAY

        while start_uid < END_ID:
            batch_end = min(start_uid + BATCH_SIZE, END_ID)
            asyncio.create_task(fetch_data(session, start_uid, batch_end, request_id))
            start_uid += BATCH_SIZE
            request_id += 1
            print_stats(current_requests_delay)
            await asyncio.sleep(current_requests_delay)

if __name__ == "__main__":
    asyncio.run(main())