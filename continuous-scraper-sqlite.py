import asyncio
import time
from tqdm import tqdm
import shutil
import sqlite3
import os
import httpx
import random
import aiohttp

'''



Roblox Continuous Game Scraper (SQlite)
by nouhidev



'''

_VERSION = "0.0.2-continuous-sqlite"

# ------- [ Scraping Parameters ] -------

# https://games.roblox.com/v1/games/votes?universeIds=
# https://games.roblox.com/v1/games?universeIds=

BASE_URL = "https://games.roblox.com/v1/games?universeIds="

# UIDs/Url (Default: 100)
BATCH_SIZE = 80

# Initial delay between requests (Default: 0.05 --> 20reqs/s)
INITIAL_REQUESTS_DELAY = 0.07

# Multiplier by which the delay will be multiplied with on rate limit error
RATE_LIMIT_DELAY_PENALTY_INCREASE = 1.1

# Multiplier by which the delay will be multiplied with on rate limit error
RATE_LIMIT_DELAY_PENALTY_DECREASE = 4

# Max allowed delay between each request
MAX_REQUESTS_DELAY = 1

# Number of consecutive requests without rate limiting required to reset the delay between requests (Default: 100)
RATE_LIMIT_PENALTY_RESET_THRESHOLD = 100

# UID to stop scraping at (Default: 5060800000)
END_ID = 5060800000

# Amount of seconds that have to be passed before returning the data (Default: 3)
RETURN_DATA_MINIMUM_DELAY = 2

# Amount of times a request will retry to get its batch
MAX_RETRIES = 30

# Concurrent open requests cap (Default: 2000)
MAX_CONCURRENT_OPEN_REQUESTS = 2000

# Base for calculating the random retry time for each failed request
MIN_RETRY_SLEEP_TIME = 1

# A random number between these bounds will be added to the base sleeping time when retrying
RANDOM_SLEEP_TIME_BOUNDS = (1, 10)

# Whether to use httpx to make requests or aiohttp
USE_HTTPX = False

# Log Response Times
LOG_RESPONSE_TIMES = False

# -------- [ File Output Paths ] --------

COUNT_FILE_PATH = "c_start_uid.txt"

ERRORED_BATCHES_FILE_PATH = "c_errored_batches.txt"

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

# Keeps track of the games scanned in the current session
games_scanned_in_session = 0

# Keeps track of the errored requests
errored_requests = []

# Keeps track of the recovered requests
recovered_requests = []

# Keeps track of the lost requests
lost_requests = []

# Keeps track of the resolved requests
resolved_requests = 0

# Accurate UIDs/s
uids_per_second = 0

# ------------- [ Other ] ---------------

progress_bar = None
terminal_width = shutil.get_terminal_size().columns
equals_line = "=" * terminal_width

# -------- [ ANSI Escape Codes ] --------

RED = '\033[91m'
DARK_RED = '\033[0;31m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RESET = '\033[0m'
GRAY = '\033[90m'
BOLD = '\033[1m'
GOLDEN = '\033[93m'
UNDERLINE = '\033[4m'
CYAN = '\033[96m'

# ------- [ SQLite Integration ] -------

conn = sqlite3.connect('games.db')

cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS scraped_data (
        uid INTEGER PRIMARY KEY,
        place_id INTEGER,
        visits INTEGER,
        name TEXT,
        favoritedCount INTEGER
    )
''')

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

start_time = time.perf_counter()

def format_time(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

def get_progress():
    global start_uid
    terminal_width, _ = shutil.get_terminal_size()
    terminal_width -= 2
    
    goal = END_ID - start_uid

    progress_ratio = min(1.0, max(0.0, games_scanned_in_session / goal))
    num_symbols = int(terminal_width * progress_ratio)
    
    progress_bar = "(" + "-" * num_symbols + " " * (terminal_width - num_symbols) + ")"
    
    return (progress_bar, goal)

def print_stats(_current_requests_delay):
    global games_added_in_session, games_scanned_in_session, errored_requests, recovered_requests, resolved_requests, lost_requests, consecutive_no_rate_limit, start_uid, uids_per_second
    os.system('cls')
    elapsed_time = time.perf_counter() - start_time
    formatted_elapsed_time = format_time(elapsed_time)

    days_left = round(get_progress()[1]/max(24*60*60*(games_scanned_in_session/elapsed_time), 0.001))
    days_left_str = f"{round(get_progress()[1]/max(24*60*60*(games_scanned_in_session/elapsed_time), 0.001))} days"
    if (days_left < 0): days_left_str = "less than a day"

    print(f"{GRAY}{equals_line}{RESET}")
    print(f"{CYAN}Session Progress: {100*min(1.0, max(0.0, games_scanned_in_session / get_progress()[1])):.12f}%{RESET} {GRAY}({games_scanned_in_session:,}/{get_progress()[1]:,} | Total Progress: {100*min(1.0, max(0.0, start_uid / END_ID)):.12f}%{RESET})")
    print(f"{GRAY}Estimate: {days_left_str} left{RESET}")
    print(CYAN + get_progress()[0] + RESET)
    print(f"{GRAY}{equals_line}{RESET}")
    print(f"Ongoing requests: {(unresolved_requests):,} {GRAY}(Closed requests: {(resolved_requests):,} | Total requests: {(unresolved_requests+resolved_requests):,}){RESET}")
    print(f"- Games added in session: {games_added_in_session:,} out of {games_scanned_in_session:,} scanned games\n")
    print(f"Average session speed: {UNDERLINE}{round(games_scanned_in_session/elapsed_time, 3):,} UIDs/s{RESET}{GRAY} > {round(60*(games_scanned_in_session/elapsed_time), 3):,} UIDs/min > {round(60*60*(games_scanned_in_session/elapsed_time), 3):,} UIDs/h > {round(24*60*60*(games_scanned_in_session/elapsed_time), 3):,} UIDs/d => {RESET}{round(END_ID/max(24*60*60*(games_scanned_in_session/elapsed_time), 0.001))}d for all")
    print(f"- Received {uids_per_second} UIDs last second")
    print(f"- Delay between new requests: {_current_requests_delay} seconds")
    print(f"{GRAY}{equals_line}{RESET}")
    print(f"{DARK_RED}Errored connections: {len(errored_requests)}{RESET}")
    print(f"- Recovered failed requests: {len(recovered_requests)}{RESET}")
    print(f"- Lost requests: {len(lost_requests)}{RESET}")
    print(f"{GRAY}{equals_line}{RESET}")
    print(f"{GRAY}{formatted_elapsed_time} | {round(1/_current_requests_delay, 3)} reqs/s | {consecutive_no_rate_limit} sCReqs | HTTPX {USE_HTTPX}{RESET}")
    print(f"{GRAY}{equals_line}{RESET}")

async def fetch_data(session, batch_start, batch_end, request_id):
    global consecutive_no_rate_limit, current_requests_delay, unresolved_requests, games_added_in_session, games_scanned_in_session, errored_requests, resolved_requests, lost_requests, uids_per_second

    unresolved_requests += 1

    retry_counter = 0

    universe_ids = ",".join(str(i) for i in range(batch_start, batch_end))
    url = BASE_URL + universe_ids
    
    # Decrease the current delay between requests after consecutive successful requests
    if consecutive_no_rate_limit >= RATE_LIMIT_PENALTY_RESET_THRESHOLD:
        current_requests_delay = max(current_requests_delay / RATE_LIMIT_DELAY_PENALTY_DECREASE, INITIAL_REQUESTS_DELAY)
        consecutive_no_rate_limit = 0

    while retry_counter < MAX_RETRIES:
        # Try to sent the HTTP request
        try:
            timer = time.perf_counter()
            response = await session.get(url, timeout=360)
            timer_end = time.perf_counter()
            if LOG_RESPONSE_TIMES: print(f"Response time: {timer_end-timer} seconds")
        # HTTP request error handling
        except Exception as e:
            retry_counter += 1
            await asyncio.sleep(MIN_RETRY_SLEEP_TIME + random.uniform(RANDOM_SLEEP_TIME_BOUNDS[0], RANDOM_SLEEP_TIME_BOUNDS[1]))
            continue

        if USE_HTTPX:
            # Rate limit handling for v1/games and v1/votes endpoints
            if response.status_code == 503 or response.status_code == 429:
                # Penalize the script for getting rate limited
                current_requests_delay = min(current_requests_delay * RATE_LIMIT_DELAY_PENALTY_INCREASE, MAX_REQUESTS_DELAY)
                consecutive_no_rate_limit = 0
                retry_counter += 1
                if f"_id{request_id}" not in errored_requests: errored_requests.append(f"_id{request_id}")
                await asyncio.sleep(MIN_RETRY_SLEEP_TIME + random.uniform(RANDOM_SLEEP_TIME_BOUNDS[0], RANDOM_SLEEP_TIME_BOUNDS[1]))
                continue
        else:
            # Rate limit handling for v1/games and v1/votes endpoints
            if response.status == 503 or response.status == 429:
                # Penalize the script for getting rate limited
                current_requests_delay = min(current_requests_delay * RATE_LIMIT_DELAY_PENALTY_INCREASE, MAX_REQUESTS_DELAY)
                consecutive_no_rate_limit = 0
                retry_counter += 1
                if f"_id{request_id}" not in errored_requests: errored_requests.append(f"_id{request_id}")
                await asyncio.sleep(MIN_RETRY_SLEEP_TIME + random.uniform(RANDOM_SLEEP_TIME_BOUNDS[0], RANDOM_SLEEP_TIME_BOUNDS[1]))
                continue
        
        # Try to parse the response as JSON
        try:
            if USE_HTTPX:
                data = response.json()
            else:
                data = await response.json()

            # Success !

            saved_progress = 0
            with open(COUNT_FILE_PATH, "r") as count_file:
                saved_progress = int(count_file.read().strip())
            with open(COUNT_FILE_PATH, "w") as count_file:
                if not saved_progress: saved_progress = 0
                count_file.write(str(max(batch_end, saved_progress)))

            consecutive_no_rate_limit += 1

            documents_to_insert = []

            for entry in data["data"]:
                uids_per_second += 1

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
                games_scanned_in_session += 1
            
            if documents_to_insert:
                for doc in documents_to_insert:
                    cursor.execute('''
                        INSERT INTO scraped_data (uid, place_id, visits, name, favoritedCount)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (doc["uid"], doc["placeId"], doc["visits"], doc["name"], doc["favoritedCount"]))
                conn.commit()
            unresolved_requests -= 1
            resolved_requests += 1
            if retry_counter > 0:
                for id in errored_requests:
                    if id == f"_id{request_id}":
                        recovered_requests.append(f"_id{request_id}")
                errored_requests = [id for id in errored_requests if id != f"_id{request_id}"]
            return
        # If the parsing fails retry
        except Exception as e:
            for id in errored_requests:
                    if id == f"_id{request_id}":
                        lost_requests.append(f"_id{request_id}")
            errored_requests = [id for id in errored_requests if id != f"_id{request_id}"]
            print(e)
            retry_counter += 1
            await asyncio.sleep(MIN_RETRY_SLEEP_TIME + random.uniform(RANDOM_SLEEP_TIME_BOUNDS[0], RANDOM_SLEEP_TIME_BOUNDS[1]))
            continue
    
    # All retries have failed
    unresolved_requests -= 1
    resolved_requests += 1
    if f"_id{request_id}" not in lost_requests: lost_requests.append(f"_id{request_id}")
        
async def main():
    global start_uid, errored_requests, recovered_requests, lost_requests, current_requests_delay, unresolved_requests, resolved_requests, uids_per_second, uids_per_second_smooth

    saved_progress = 0
    with open(COUNT_FILE_PATH, "r") as count_file:
        saved_progress = int(count_file.read().strip())
    if not saved_progress: saved_progress = 0
    start_uid = saved_progress
    print(f"{UNDERLINE}{CYAN}Starting continuous scraper:{RESET}")
    print(f"{GRAY}- Targetting {BASE_URL}{RESET}")
    print(f"{GRAY}  - with a batch size of {BATCH_SIZE} UIDs/Url{RESET}")
    print(f"{GRAY}  - at the starting UID {start_uid:,}{RESET}")
    if USE_HTTPX:
        print(f"{GRAY}- Requesting using HTTPX{RESET}")
    else:
        print(f"{GRAY}- Requesting using AIOHTTP{RESET}")
    print(f"{GRAY}  - with a cap of {MAX_CONCURRENT_OPEN_REQUESTS} open requests{RESET}")
    print(f"{GRAY}- Desired delay between new requests {INITIAL_REQUESTS_DELAY} seconds ({round(1/INITIAL_REQUESTS_DELAY, 2)} reqs/s){RESET}")

    await asyncio.sleep(3)

    if USE_HTTPX:
        async with httpx.AsyncClient(http2=True, limits=httpx.Limits(max_connections=None, max_keepalive_connections=0)) as session:
            request_id = 1
            current_requests_delay = INITIAL_REQUESTS_DELAY

            last_print_time = time.perf_counter()

            while start_uid < END_ID:
                if time.perf_counter() - last_print_time >= 1:
                    print_stats(current_requests_delay)
                    last_print_time = time.perf_counter()
                    uids_per_second = 0

                if unresolved_requests < MAX_CONCURRENT_OPEN_REQUESTS:
                    batch_end = min(start_uid + BATCH_SIZE, END_ID)
                    asyncio.create_task(fetch_data(session, start_uid, batch_end, request_id))
                    start_uid += BATCH_SIZE
                    request_id += 1
                await asyncio.sleep(current_requests_delay)
    else:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0, limit_per_host=0, force_close=True)) as session:
            request_id = 1
            current_requests_delay = INITIAL_REQUESTS_DELAY

            last_print_time = time.perf_counter()

            while start_uid < END_ID:
                if time.perf_counter() - last_print_time >= 1:
                    print_stats(current_requests_delay)
                    last_print_time = time.perf_counter()
                    uids_per_second = 0

                if unresolved_requests < MAX_CONCURRENT_OPEN_REQUESTS:
                    batch_end = min(start_uid + BATCH_SIZE, END_ID)
                    asyncio.create_task(fetch_data(session, start_uid, batch_end, request_id))
                    start_uid += BATCH_SIZE
                    request_id += 1
                await asyncio.sleep(current_requests_delay)

if __name__ == "__main__":
    asyncio.run(main())