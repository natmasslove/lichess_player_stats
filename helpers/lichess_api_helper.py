import io
from datetime import datetime
import enum


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
    WhiteFideId: Optional[str] = None
    BlackFideId: Optional[str] = None
    Termination: Optional[str] = None

    GameId: Optional[str] = None

    FEN: Optional[str] = None
    SetUp: Optional[str] = None


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


class ChessPerfType(enum.Enum):
    ULTRA_BULLET = "ultraBullet"
    BULLET = "bullet"
    BLITZ = "blitz"
    RAPID = "rapid"
    CLASSICAL = "classical"
    CORRESPONDENCE = "correspondence"
    CHESS960 = "chess960"
    CRAZYHOUSE = "crazyhouse"
    ANTICHESS = "antichess"
    ATOMIC = "atomic"
    HORDE = "horde"
    KING_OF_THE_HILL = "kingOfTheHill"
    RACING_KINGS = "racingKings"
    THREE_CHECK = "threeCheck"


class LichessAPIHelper(Exception):
    pass


class LichessAPIHelper:
    def __init__(self, lichess_api_token: str):
        self.lichess_api_token = lichess_api_token
        self.base_url = BASE_URL
        self.req_helper = Urllib3Helper(BASE_URL)

    @classmethod
    def generate_timestamps_msec(cls, start_date: str, end_date: str):
        def parse_date(date_str):
            try:
                return datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD.")

        start_dt = parse_date(start_date)
        end_dt = parse_date(end_date)

        # Set times for start and end
        start_timestamp = (
            int(start_dt.replace(hour=0, minute=0, second=0).timestamp()) * 1000
        )
        end_timestamp = (
            int(end_dt.replace(hour=23, minute=59, second=59).timestamp()) * 1000
        )

        return start_timestamp, end_timestamp

    @classmethod
    def validate_perf_types(cls, perf_types_str: str | None) -> str:
        """
        Validates a comma-separated string of chess perf-types (as of Lichess terminology).
        Trims spaces, and returns a cleaned string
        """
        if not perf_types_str:  # Handles None or empty string
            return None

        valid_values = {perf_type.value for perf_type in ChessPerfType}
        in_perf_types = [perf_type.strip() for perf_type in perf_types_str.split(",")]

        invalid_perf_types = [
            perf_type for perf_type in in_perf_types if perf_type not in valid_values
        ]

        if invalid_perf_types:
            valid_list = ", ".join(valid_values)
            raise ValueError(
                f"PerfType(s) {', '.join(invalid_perf_types)} is not valid. Should be one of the following: {valid_list}"
            )

        return ",".join(in_perf_types)

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

            try:
                output.append(PGNGameHeader(**dict(game.headers)))
            except Exception as e:
                print(f"Error parsing game headers: {e}")
                print(dict(game.headers))
                raise e

        return output
