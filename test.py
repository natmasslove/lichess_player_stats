import os
import json
import dotenv
from helpers.lichess_api_helper import LichessAPIHelper

dotenv.load_dotenv()
lichess_token = os.environ["LICHESS_TOKEN"]


if __name__ == "__main__":
    username = "masslove"
    api_helper = LichessAPIHelper(lichess_token=lichess_token)

    api_url = f"api/games/user/{username}"
    params = {"max": 3, "moves": False, "pgnInJson": True, "opening": True}
    response = api_helper.exec_request_ndjson(api_url, params)

    for line in response:
        print("***")
        json_data = json.loads(line)
        print(json.dumps(json_data, indent=4, default=str))

    # lines = response.text.splitlines()
    # for line in lines:
    #     print("****")
    #     print(line)

    # print(response.text)
    # print(json.dumps(response, indent=4, default=str))
