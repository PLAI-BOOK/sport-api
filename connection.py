import http.client
import json
import sys
from dotenv import load_dotenv
import os
conn=None
api_key=""
base_url=""
headers=""

# this will make the api call easier
# this will connect to the api
def _connect():
    global conn,api_key,base_url,headers
    # Ensure stdout is UTF-8 encoded
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
    # Load the .env file
    load_dotenv()
    # test
    api_key =os.getenv('API_KEY')
    base_url = os.getenv('BASE_URL')
    conn = http.client.HTTPSConnection(base_url)
    headers = {
        'x-rapidapi-host': base_url,
        'x-rapidapi-key': api_key
    }

# this will make the api call and save the response to a file in the directory you call it from with a dynamic name
def call_api(params):
    global conn, headers
    if not conn:
        _connect()

    page = 1  # Start with the first page
    total_pages = 1  # Default, will be updated after first call
    all_responses = []  # List to store responses from all pages

    while page <= total_pages:
        try:
            # Make the API call without pagination on the first request
            paginated_params = f"{params}" if page == 1 else f"{params}&page={page}"

            conn.request("GET", paginated_params, headers=headers)
            res = conn.getresponse()
            data = res.read()

            # Decode the JSON response
            response_data = json.loads(data.decode("utf-8"))

            # Add the response to the list of all responses
            all_responses.append(response_data)

            # Generate a unique filename for each page
            safe_filename = f"{base_url}{paginated_params.replace('?', '_').replace('&', '_').replace('=', '-').replace('/', '-')}.json"

            # Save the JSON response to a file
            # with open(safe_filename, 'w', encoding='utf-8') as file:
            #     json.dump(response_data, file, ensure_ascii=False, indent=4)

            # Extract pagination info (total pages and current page)
            if page == 1:
                current_page = response_data.get("paging", {}).get("current", 1)
                total_pages = response_data.get("paging", {}).get("total", 1)

                # Print pagination info for debugging
                # print(f"Total Pages: {total_pages}, Current Page: {current_page}")

            page += 1  # Increment the page number for the next iteration

        except Exception as e:
            print(f"Error: {e}")
            return

    # Return all the responses after fetching all pages
    return all_responses