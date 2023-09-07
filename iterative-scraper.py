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
Roblox Game Scraper
by nouhidev

Special Thanks:
- Astar114
- yqat
'''

_VERSION = "0.0.5-iterative"

# ------- [ Scraping Parameters ] -------

# https://games.roblox.com/v1/games/votes?universeIds=
# https://games.roblox.com/v1/games?universeIds=

BASE_URL = "https://games.roblox.com/v1/games?universeIds="

# UIDs/Url (Default: 100)
batch_size = 100

# Concurrent Requests (Default: 100)
concurrent_requests = 100

# UID to stop scraping at (Default: 5060800000)
END_ID = 5060800000

# Amount of seconds that have to be passed before returning the data (Default: 3)
rate_limit_delay = 2

# Whether to dynamically increase/decrease the concurrent requests based on rate limiting
ADAPTIVE_CONCURRENT_REQUESTS = False

# Whether to dynamically increase/decrease the rate_limit_delay based on unusual response times
ADAPTIVE_RATE_LIMIT_DELAY = False

# Approximation of the highest UID
CURRENT_KNOWN_END_UID = 5071111000

# After each request wait this amount of seconds until making a new one (Default: 0.001)
CONCURRENT_REQUEST_SPAWN_DELAY = 0.001

# Delay between iterations (Default: 1.0)
TIME_BETWEEN_ITERATIONS = 1.0

# Display intensive debug information (WIP)
DISPLAY_DEBUG_INFORMATION = False

# Leave out calculations and unnecessary printing
PERFORMANCE_MODE = True

# ------------- [ Filter ] --------------

# Whether to apply filters or not
ENABLE_FILTER = True

# Only accept games with any of the desired strings in their name
DESIRED_STRINGS = []

# Whether to filter for desired strings or not
CHECK_FOR_DESIRED_STRINGS = False

# Only accept games with any of the desired genres selected
DESIRED_GENRES = ["Horror"]

# Whether to filter for desired genres or not
CHECK_FOR_DESIRED_GENRES = True

# Only accept games with more than the desired min visits count
DESIRED_MIN_VISITS = 1000

# Only accept games with less than the desired max visits count
DESIRED_MAX_VISITS = float('inf')

# Reject games with any of the banned strings in their name
BANNED_STRINGS = ["s' Place, s' place"]

# Whether to check for games that have never been updated or not
CHECK_FOR_CREATED_UPDATED_EQUALITY = True

# ----- [ ----------------------- ] -----




'''
Only change settings below this point if
you know what you're doing.
'''




# -------- [ File Output Paths ] --------

COUNT_FILE_PATH = "start_uid.txt"

ERRORED_BATCHES_FILE_PATH = "errored_batches.txt"

# -- [ Adaptive Concurrent Requests ] ---

MIN_CONCURRENT_REQUESTS = 10

CONCURRENT_REQUESTS_INCREASE = 10

# --- [ Adaptive Rate Limit Delay ] -----

MIN_RATE_LIMIT_DELAY = 1

RATE_LIMIT_DELAY_INCREASE = 3

last_request_time = 0
response_time_threshold = 0 
suspected_rate_limit_count = 0
average_response_time = 0
response_time_count = 0
response_time_threshold_multiplier = 2

# -------- [ Statistics Related ] -------

# Keeps track of current iteration of a session
current_iteration = 0

# Keeps track of how many games were scanned in a session
games_scanned = 0

# Keeps track of how many games were lost due to rate limiting
loss_count = 0 

# Keeps track of how many games were lost due to requesting errors
requesting_loss_count = 0 

# Stores the UIDs/s of the previous iteration
previous_uids_per_second = 0

# Stores all UIDs/s of each iteration
uids_per_second_saved = []

# Keeps track of the time taken for ensuring the minimum time before returning data
rate_limit_precaution_elapsed = 0

# -------- [ Requests Managing ] --------

# Locally used to keep track of the start of each batch
start_id = 0

# Used to keep track of the scrapers progress
requested_count = 0

# Keeps track of the request made in each iteration
current_request = 0

# Checks whether any request in an iteration has been rate limited
rateLimited = False

# After how many seconds each httpx request should time out
MAX_HTTPX_TIMEOUT = 20

# How many times the scraper should retry a failed batch until giving up
MAX_RETRIES = 3

# How many seconds the scraper should wait until retrying
RETRY_DELAY = 2

# Specifies what headers to send with the GET request
GET_REQUEST_HEADERS = {}

lost_batches = 0
recovered_batches = 0

# ------------- [ Other ] ---------------

progress_bar = None
terminal_width = shutil.get_terminal_size().columns
equals_line = "=" * terminal_width
dump_steps = 2

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
            requested_count = int(count_str)
except FileNotFoundError:
    with open(COUNT_FILE_PATH, "w") as count_file:
        count_file.write(str(requested_count))

# ----- [ ----------------------- ] -----

async def fetch_games(session, batch_start, batch_end):
    global last_request_time, rate_limit_delay, suspected_rate_limit_count, average_response_time, response_time_count, concurrent_requests, rateLimited, loss_count, rate_limit_precaution_elapsed, requesting_loss_count, current_request, lost_batches, recovered_batches

    # Construct the request URL
    universe_ids = ",".join(str(i) for i in range(batch_start, batch_end))
    url = BASE_URL + universe_ids

    current_request += 1

    retry_counter = 0

    local_rate_limit = False

    await asyncio.sleep(current_request*CONCURRENT_REQUEST_SPAWN_DELAY)

    while retry_counter < MAX_RETRIES:
        start_time = time.time()

        if DISPLAY_DEBUG_INFORMATION: print(f"{GRAY}DEBUG: Requesting batch {batch_start}-{batch_end}{RESET}")

        # Try to GET the constructed request URL
        try:
            response = await session.get(url, timeout=MAX_HTTPX_TIMEOUT, headers=GET_REQUEST_HEADERS)
        # If that fails retry 
        except Exception as e:
            retry_counter += 1
            requesting_loss_count += 1
            await asyncio.sleep(RETRY_DELAY)
            continue

        response_time = time.time() - start_time

        average_response_time = (average_response_time * response_time_count + response_time) / (response_time_count + 1)
        response_time_count += 1

        response_time_threshold = average_response_time * response_time_threshold_multiplier

        if response.status_code == 503 or response.status_code == 429:
            if not local_rate_limit and ADAPTIVE_CONCURRENT_REQUESTS: concurrent_requests = max(MIN_CONCURRENT_REQUESTS, concurrent_requests - CONCURRENT_REQUESTS_INCREASE)
            if not local_rate_limit and ADAPTIVE_RATE_LIMIT_DELAY: rate_limit_delay += RATE_LIMIT_DELAY_INCREASE

            rateLimited = True
            local_rate_limit = True

            retry_counter += 1
            print(f" {BOLD}{DARK_RED}Lost batch {batch_start}-{batch_end} due to rate limiting [{response.status_code}]{RESET}")
            lost_batches += 1
            await asyncio.sleep(RETRY_DELAY)
            continue

        # If the response time is unusually high --> suspect rate limiting or inadequate rate limit delay
        if (response_time > response_time_threshold) and ADAPTIVE_RATE_LIMIT_DELAY:
            suspected_rate_limit_count += 1

        progress_bar.update(1)

        rate_limit_precaution_time = time.time()

        # If the data returned earlier than the minimum waiting time --> sleep until it's over
        if time.time() - last_request_time < rate_limit_delay:
            await asyncio.sleep(rate_limit_delay - (time.time() - last_request_time))

        rate_limit_precaution_elapsed = time.time() - rate_limit_precaution_time

        last_request_time = time.time()

        # Attempt to parse the response to JSON
        try:
            data = response.json()
            if local_rate_limit:
                recovered_batches += 1
                print(f" {GREEN}Successfully recovered batch {batch_start}-{batch_end} ({recovered_batches}/{lost_batches}){RESET}")
            return data.get("data", [])
        # If the parsing fails retry
        except Exception as e:
            await asyncio.sleep(RETRY_DELAY)
            continue
    
    # If all retrying attempts fail --> return an empty array and save the failed batch
    loss_count += 1
    print(f" {RED}Could not recover batch {batch_start}-{batch_end}{RESET}")
    formatted = f"{batch_start}-{batch_end}\n"
    try:
        with open(ERRORED_BATCHES_FILE_PATH, "a") as file:
            file.write(formatted)
    except FileNotFoundError:
        with open(ERRORED_BATCHES_FILE_PATH, "w") as file:
            file.write(formatted)
    return []


async def main():
    global requested_count, current_iteration, concurrent_requests, progress_bar, start_id, suspected_rate_limit_count, rate_limit_delay, rateLimited, average_response_time, response_time_threshold_multiplier, previous_uids_per_second, dump_progress, batch_size, rate_limit_delay, games_scanned, bulk_operations, rate_limit_precaution_elapsed, requesting_loss_count, current_request, lost_batches, recovered_batches

    async with httpx.AsyncClient(http2=True, limits=httpx.Limits(max_connections=None, max_keepalive_connections=0)) as session:
        while start_id < END_ID:
            print(f"{GRAY}{equals_line}{RESET}")
            print(f"{BOLD}{GOLDEN}{UNDERLINE}Iteration {current_iteration+1}{RESET}:")
            print(f"{GRAY}[{batch_size} UIDs/Url; {concurrent_requests} cReqs]{RESET}")

            dump_progress = 0

            tasks = []

            start_time = time.time()

            start_id = requested_count

            progress_bar = tqdm(total=concurrent_requests, unit="req", desc=f"Requesting Batch {start_id:,}-{start_id+batch_size*concurrent_requests:,} ({(batch_size*concurrent_requests):,})")

            # Reset the rate limited flag for this iteration
            rateLimited = False

            current_request = 1

            lost_batches = 0

            recovered_batches = 0

            # Initialize all concurrent requests
            for _ in range(concurrent_requests):
                if start_id >= END_ID:
                    break
                batch_end = min(start_id + batch_size, END_ID)
                tasks.append(fetch_games(session, batch_start=start_id, batch_end=batch_end))
                start_id += batch_size

            batch_results = await asyncio.gather(*tasks)

            progress_bar.close()

            if lost_batches > 0:
                if recovered_batches == lost_batches and lost_batches:
                    print(f" {BOLD}{GREEN}Successfully recovered all {lost_batches} lost batches{RESET}")
                else:
                    print(f" {BOLD}{RED}Could not recover {lost_batches-recovered_batches} batches{RESET}")


            dump_progress_bar = tqdm(total=dump_steps, unit="steps", desc=f"Updating database")

            elapsed_time = time.time() - start_time

            bulk_operations = []

            games = []
            for batch_data in batch_results:
                for game in batch_data:
                    games_scanned += 1
                    if (
                        (not CHECK_FOR_DESIRED_GENRES or game.get("genre") in DESIRED_GENRES) 
                        and DESIRED_MIN_VISITS <= game.get("visits") <= DESIRED_MAX_VISITS
                        and all(banned_string not in game.get("name") for banned_string in BANNED_STRINGS)
                        and (not CHECK_FOR_CREATED_UPDATED_EQUALITY or game.get("created") != game.get("updated"))
                        and (not CHECK_FOR_DESIRED_STRINGS or any(desired_string in game.get("name") for desired_string in DESIRED_STRINGS))
                    ) or not ENABLE_FILTER:
                        game_info = {
                            "uid": game.get("id"),
                            "name": game.get("name"),
                            "visits": game.get("visits"),
                            "placeId": game.get("rootPlaceId"),
                            "favoritedCount": game.get("favoritedCount")
                        }
                        games.append(game_info)
                        bulk_operations.append(InsertOne(game_info))

            dump_progress_bar.update(1)

            requested_count += batch_size*concurrent_requests

            with open(COUNT_FILE_PATH, "w") as count_file:
                count_file.write(str(requested_count))

            dump_start_time = time.time()

            if bulk_operations:
                await collection.bulk_write(bulk_operations)

            dump_progress_bar.update(1)

            dump_elapsed_time = time.time() - dump_start_time

            uids_per_second = (batch_size*concurrent_requests)/elapsed_time

            uids_per_second_saved.append({'uids_per_second': uids_per_second, 'concurrent_requests': concurrent_requests, 'elapsed_time': elapsed_time})

            uids_per_second_string = ""

            # Keep track of the UIDs/s compared to the previous iteration
            uids_per_second_difference = uids_per_second - previous_uids_per_second
            if uids_per_second_difference > 0:
                uids_per_second_string = f"{BOLD}{GREEN}+{round(uids_per_second_difference, 2)}{RESET}"
            else:
                uids_per_second_string = f"{BOLD}{RED}{round(uids_per_second_difference, 2)}{RESET}"

            previous_uids_per_second = uids_per_second

            dump_progress_bar.close()

            if not PERFORMANCE_MODE:
                print(f"{CYAN}Detailed Durations:{RESET}")
                print(f"- Requesting took {round(elapsed_time, 3)} seconds (<= {round(average_response_time * response_time_threshold_multiplier, 3)} s)")
                print(f"- Rate Limit Precautions took {round(rate_limit_precaution_elapsed, 3)} seconds")
                print(f"- Dumping took {round(dump_elapsed_time, 3)} seconds")
                print(f"= Total: {BOLD}{round(elapsed_time+rate_limit_precaution_elapsed+dump_elapsed_time, 3)} seconds{RESET} ({UNDERLINE}{round(uids_per_second, 3)} UIDs/s{RESET} | {uids_per_second_string}){RESET}")

            if ADAPTIVE_CONCURRENT_REQUESTS or ADAPTIVE_CONCURRENT_REQUESTS:print(f"{CYAN}Optimization Updates:{RESET}")
            if ADAPTIVE_CONCURRENT_REQUESTS:print(f"- Adaptive Rate Limiting:")
            if not rateLimited and ADAPTIVE_CONCURRENT_REQUESTS:
                print(f"{GREEN}  - No confirmed rate limit detected. Increasing maximum concurrent requests from {concurrent_requests} to {concurrent_requests+CONCURRENT_REQUESTS_INCREASE}{RESET}")
                concurrent_requests += CONCURRENT_REQUESTS_INCREASE

            # Adjust concurrent_requests based on rate limiting
            if suspected_rate_limit_count > 0:
                if ADAPTIVE_CONCURRENT_REQUESTS:
                    print(f"{RED}  - Response time exceeded the controlled threshold. Decreasing concurrent requests from {concurrent_requests} to {max(MIN_CONCURRENT_REQUESTS, concurrent_requests - CONCURRENT_REQUESTS_INCREASE)}{RESET}")
                    concurrent_requests = max(MIN_CONCURRENT_REQUESTS, concurrent_requests - CONCURRENT_REQUESTS_INCREASE)
                if ADAPTIVE_RATE_LIMIT_DELAY:
                    print(f"{RED}  - Suspecting rate limiting. Increasing the request min time from {rate_limit_delay} s to {rate_limit_delay + RATE_LIMIT_DELAY_INCREASE} s{RESET}")
                    rate_limit_delay += RATE_LIMIT_DELAY_INCREASE
                    suspected_rate_limit_count = 0
            else:
                if (rate_limit_delay - RATE_LIMIT_DELAY_INCREASE >= MIN_RATE_LIMIT_DELAY) and ADAPTIVE_RATE_LIMIT_DELAY:
                    rate_limit_delay -= RATE_LIMIT_DELAY_INCREASE
                    print(f"{GREEN}  - Not suspecting rate limiting. Decreasing the request min time from {rate_limit_delay + RATE_LIMIT_DELAY_INCREASE} s to {rate_limit_delay} s{RESET}")

            num_entries = await collection.count_documents({})
            if not PERFORMANCE_MODE:
                print(f"{CYAN}Database Updates:{RESET}")
                print(f"- Found {UNDERLINE}{len(games):,}{RESET} new matching games")
                print(f"- Lost {loss_count:,} ({round((loss_count/games_scanned)*100, 2)}%) games to rate limiting")
                print(f"- Lost {requesting_loss_count:,} ({round((requesting_loss_count/games_scanned)*100, 2)}%) games to requesting errors")
                print(f"- Currently storing {BOLD}{num_entries:,}{RESET} games")
                print(f"{CYAN}Progress:{RESET}")
                print(f"- {((requested_count - batch_size*concurrent_requests)/CURRENT_KNOWN_END_UID):.10f}%")
                max_uids_per_second = round(max(item['uids_per_second'] for item in uids_per_second_saved), 3)
                max_uids_index = max(range(len(uids_per_second_saved)), key=lambda i: uids_per_second_saved[i]['uids_per_second'])
                concurrent_requests = uids_per_second_saved[max_uids_index]['concurrent_requests']
                print(f"{GRAY}(Request Min Time: {rate_limit_delay} s | Best Performance: {max_uids_per_second} UIDs/s at {concurrent_requests} cReqs with {batch_size} UIDs){RESET}")
            else:
                print(len(games) + " matches found")

            current_iteration += 1
            print(f"{GRAY}{equals_line}{RESET}")

            await asyncio.sleep(TIME_BETWEEN_ITERATIONS)

if __name__ == "__main__":
    asyncio.run(main())
