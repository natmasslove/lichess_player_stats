import os
import json
import dotenv
from helpers.lichess_api_helper import LichessAPIHelper
from helpers.urllib3_helper import Urllib3Helper

dotenv.load_dotenv()
lichess_token = os.environ["LICHESS_TOKEN"]
BASE_URL = "https://lichess.org/"


if __name__ == "__main__":
    username = "masslove"

    req_helper = Urllib3Helper(BASE_URL)

    api_path = f"api/games/user/{username}"
    params = {"max": 3, "moves": False, "pgnInJson": False, "opening": True}
    params = {"max": 3, "moves": False}

    response = req_helper.get(api_path, params=params, token=lichess_token)
    print(response)

    # api_helper = LichessAPIHelper(lichess_token=lichess_token)

    # api_url = f"api/games/user/{username}"
    # params = {"max": 3, "moves": False, "pgnInJson": False, "opening": True}
    # response = api_helper.exec_request_ndjson(api_url, params)
    # for line in response:
    #     print("***")
    #     json_data = json.loads(line)
    #     print(json.dumps(json_data, indent=4, default=str))

    # lines = response.text.splitlines()
    # for line in lines:
    #     print("****")
    #     print(line)

    # print(response.text)
    # print(json.dumps(response, indent=4, default=str))
