import io
from datetime import datetime

from typing import Optional, TypedDict
from dataclasses import dataclass

from chess.pgn import read_game

from helpers.urllib3_helper import Urllib3Helper

BASE_URL = "https://lichess.org/"


@dataclass
class PGNGameHeader:
    Event: Optional[str] = None
    Site: Optional[str] = None
    Date: Optional[str] = None
    Round: Optional[str] = None

    White: Optional[str] = None
    Black: Optional[str] = None
    Result: Optional[str] = None

    UTCDate: Optional[str] = None
    UTCTime: Optional[str] = None

    Score: Optional[str] = None
    WhiteElo: Optional[str] = None
    BlackElo: Optional[str] = None
    WhiteRatingDiff: Optional[str] = None
    BlackRatingDiff: Optional[str] = None

    Variant: Optional[str] = None
    TimeControl: Optional[str] = None

    ECO: Optional[str] = None
    Opening: Optional[str] = None

    WhiteTitle: Optional[str] = None
    BlackTitle: Optional[str] = None
    Termination: Optional[str] = None

    GameId: Optional[str] = None


class APIParams_GetGames(TypedDict):
    # >= 1356998400070 Download games played since this timestamp. Defaults to account creation date
    since: Optional[int]

    # >= 1356998400070 Download games played until this timestamp. Defaults to now.
    until: Optional[int]

    # >= 1 How many games to download. Leave empty to download all games.
    max: Optional[int]

    # [Filter] Only rated (true) or casual (false) games
    rated: Optional[bool]

    # Default: null
    # Enum: "ultraBullet" "bullet" "blitz" "rapid" "classical" "correspondence" "chess960" "crazyhouse" "antichess" "atomic" "horde" "kingOfTheHill" "racingKings" "threeCheck"
    # [Filter] Only games in these speeds or variants. Multiple perf types can be specified, separated by a comma. Example: blitz,rapid,classical
    perfType: Optional[str]

    # Default: true
    # Include the PGN moves.
    moves: Optional[bool]

    # Default: false
    # Include the full PGN within the JSON response, in a pgn field. The response type must be set to application/x-ndjson by the request Accept header.
    pgnInJson: Optional[bool]

    # Default: false
    # Include the opening name. Example: [Opening "King's Gambit Accepted, King's Knight Gambit"]
    opening: Optional[bool]


class LichessAPIHelper(Exception):
    pass


class LichessAPIHelper:
    def __init__(self, lichess_api_token: str):
        self.lichess_api_token = lichess_api_token
        self.base_url = BASE_URL
        self.req_helper = Urllib3Helper(BASE_URL)

    @classmethod
    def generate_timestamps_msec(cls, start_date: str, end_date: str):
        # Convert string to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Set times for start and end
        start_timestamp = (
            int(start_dt.replace(hour=0, minute=0, second=0).timestamp()) * 1000
        )
        end_timestamp = (
            int(end_dt.replace(hour=23, minute=59, second=59).timestamp()) * 1000
        )

        return start_timestamp, end_timestamp

    def get_games_headers(
        self, username: str, params: APIParams_GetGames
    ) -> list[PGNGameHeader]:
        api_path = f"api/games/user/{username}"
        pgn_response = self.req_helper.get(
            api_path=api_path, params=params, token=self.lichess_api_token
        )

        # convert pgn_response to a stream readable by read_game
        pgn_stream = io.StringIO(pgn_response)

        output = []
        while True:
            game = read_game(pgn_stream)
            if game is None:
                break

            output.append(PGNGameHeader(**dict(game.headers)))

        return output
