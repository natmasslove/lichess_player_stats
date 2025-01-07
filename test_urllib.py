import urllib3
import os
import json
import dotenv

# Load environment variables
dotenv.load_dotenv()
lichess_token = os.environ["LICHESS_TOKEN"]
BASE_URL = "https://lichess.org/"

# Initialize the HTTP manager
http = urllib3.PoolManager()

# API endpoint
url = BASE_URL + "api/games/user/masslove"

# Headers for the request
headers = {
    "Authorization": f"Bearer {lichess_token}",
}

# Query parameters
query_params = {
    "moves": "false",  # Lichess API expects boolean values as strings
    "max": 3,
}

# Build the URL with query parameters
from urllib.parse import urlencode

url_with_params = f"{url}?{urlencode(query_params)}"

# Make the GET request
response = http.request(
    method="GET",
    url=url_with_params,
    headers=headers,
)

# Print the status and response data
print(response.status)
print(response.data.decode("utf-8"))
