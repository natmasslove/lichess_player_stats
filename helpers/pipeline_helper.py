from datetime import datetime, timezone
from dataclasses import fields, dataclass, asdict
from enum import Enum
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

import csv

from helpers.lichess_api_helper import PGNGameHeader


@dataclass
class TimeControlThreshold:
    name: str
    estimated_time_max: int  # In seconds


class TimeControlType(Enum):
    ULTRA_BULLET = "UltraBullet"
    HYPER_BULLET = "HyperBullet"
    BULLET = "Bullet"
    BLITZ = "Blitz"
    RAPID = "Rapid"
    CLASSIC = "Classic"


TIME_CONTROL_THRESHOLDS = [
    TimeControlThreshold(TimeControlType.ULTRA_BULLET.value, 29),
    TimeControlThreshold(TimeControlType.HYPER_BULLET.value, 59),
    TimeControlThreshold(TimeControlType.BULLET.value, 179),
    TimeControlThreshold(TimeControlType.BLITZ.value, 599),
    TimeControlThreshold(TimeControlType.RAPID.value, 1799),
    TimeControlThreshold(TimeControlType.CLASSIC.value, float("inf")),  # No upper bound
]


@dataclass
class PGNGameHeaderStandardized:
    Event: Optional[str] = None
    Site: Optional[str] = None
    Date: Optional[str] = None
    Round: Optional[str] = None

    White: Optional[str] = None
    Black: Optional[str] = None

    Result: Optional[float] = None  # "1" - White wins, "0.5" - draw, "0" - Black
    UTCDateTime: Optional[datetime] = None
    WhiteElo: Optional[int] = None
    BlackElo: Optional[int] = None
    WhiteRatingChange: Optional[float] = None
    BlackRatingChange: Optional[float] = None

    Variant: Optional[str] = None

    # User-friendly time control in format "minutes initial + seconds increment". For hyper and ultra bullet: '½', '¼'
    TimeControl: Optional[str] = None
    # Original time control in format "seconds initial + seconds increment"
    TimeControlSec: Optional[str] = None
    # Ultrabullet / Hyperbullet / Bullet / Blitz / Rapid / Classic
    TimeControlType: Optional[str] = None

    ECO: Optional[str] = None
    OpeningFamily: Optional[str] = None
    OpeningVariation: Optional[str] = None
    OpeningSubVariation: Optional[str] = None

    WhiteTitle: Optional[str] = None
    BlackTitle: Optional[str] = None
    Termination: Optional[str] = None

    @classmethod
    def join_utc_date_and_time(cls, utcdate: str, utctime: str) -> datetime:
        if utcdate is None or utctime is None:
            return None
        try:
            date_str = f"{utcdate} {utctime}"
            utc_datetime = datetime.strptime(date_str, "%Y.%m.%d %H:%M:%S")
            utc_datetime = utc_datetime.replace(tzinfo=timezone.utc)
            return utc_datetime
        except ValueError:
            raise ValueError(f"Invalid UTCDate {utcdate} or UTCTime format {utctime}")

    @classmethod
    def str_to_int(cls, value: Optional[str]) -> Optional[int]:
        if value is None or value.strip() == "":
            return None
        try:
            return int(value)
        except ValueError:
            return None

    @classmethod
    def str_to_float(cls, value: Optional[str]) -> Optional[float]:
        if value is None or value.strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None

    @classmethod
    def split_opening(
        cls, opening: str
    ) -> tuple[
        Optional[str], Optional[str], Optional[str]
    ]:  # Opening Family / Variation / Sub-Variation
        if opening is None:
            return None, None, None

        # Attempt to split the family and the variation part
        parts = opening.split(":", 1)
        family = parts[0].strip()
        variation = None
        subvariation = None

        # Parse variation
        variation_raw = parts[1] if len(parts) > 1 else None
        if variation_raw:
            # Attempt to split the variation and sub-variation
            variation_parts = variation_raw.split(",", 1)
            variation = variation_parts[0].strip()
            subvariation = (
                variation_parts[1].strip() if len(variation_parts) > 1 else None
            )

        return family, variation, subvariation

    @classmethod
    def parse_timecontrol(cls, tc: str) -> tuple[float, int]:
        """Parses a chess time control string (e.g., '180+2') into (minutes, increment)."""
        initial, increment = map(int, tc.split("+"))
        return initial / 60, increment

    @classmethod
    def format_timecontrol(cls, minutes: float, increment: int) -> str:
        """Formats the parsed time control values into a readable format."""
        fraction_map = {0.5: "½", 0.25: "¼"}
        if minutes in fraction_map:
            initial_str = fraction_map[minutes]
        elif minutes < 1:
            initial_str = "0"
        else:
            initial_str = str(int(minutes))

        return f"{initial_str}+{increment}"

    @classmethod
    def calculate_estimated_time(cls, minutes: float, increment: int) -> int:
        """Calculates estimated game time in seconds: initial time + 40 moves * increment."""
        return int(minutes * 60) + 40 * increment

    @classmethod
    def get_timecontrol_type(cls, minutes: float, increment: int) -> str:
        """Determines the time control type based on estimated game time."""
        estimated_time = cls.calculate_estimated_time(minutes, increment)

        for threshold in TIME_CONTROL_THRESHOLDS:
            if estimated_time <= threshold.estimated_time_max:
                return threshold.name

        return TimeControlType.CLASSIC.value

    @classmethod
    def from_pgn_header(cls, pgn_header: PGNGameHeader):
        outp = PGNGameHeaderStandardized()
        outp.Event = pgn_header.Event
        outp.Site = pgn_header.Site
        outp.Date = pgn_header.Date
        outp.Round = pgn_header.Round
        outp.White = pgn_header.White
        outp.Black = pgn_header.Black

        results_map = {"1-0": 1.0, "0-1": 0.0, "1/2-1/2": 0.5}
        outp.Result = results_map.get(pgn_header.Result)
        outp.UTCDateTime = cls.join_utc_date_and_time(
            pgn_header.UTCDate, pgn_header.UTCTime
        )

        outp.WhiteElo = cls.str_to_int(pgn_header.WhiteElo)
        outp.BlackElo = cls.str_to_int(pgn_header.BlackElo)

        outp.WhiteRatingChange = cls.str_to_float(pgn_header.WhiteRatingDiff)
        outp.BlackRatingChange = cls.str_to_float(pgn_header.BlackRatingDiff)

        outp.Variant = pgn_header.Variant
        outp.TimeControlSec = pgn_header.TimeControl
        mins_initial, secs_increment = cls.parse_timecontrol(pgn_header.TimeControl)
        outp.TimeControl = cls.format_timecontrol(mins_initial, secs_increment)
        outp.TimeControlType = cls.get_timecontrol_type(mins_initial, secs_increment)

        outp.ECO = pgn_header.ECO

        outp.OpeningFamily, outp.OpeningVariation, outp.OpeningSubVariation = (
            cls.split_opening(pgn_header.Opening)
        )

        outp.WhiteTitle = pgn_header.WhiteTitle
        outp.BlackTitle = pgn_header.BlackTitle
        outp.Termination = pgn_header.Termination

        return outp


@dataclass
class PGNGameHeaderPersonified:
    # PGN Game Data from a perspective of a specific game participant
    Event: Optional[str] = None
    Site: Optional[str] = None
    Date: Optional[str] = None
    Round: Optional[str] = None

    Player: Optional[str] = None
    Opponent: Optional[str] = None
    Color: Optional[str] = None  # whether Player

    Result: Optional[float] = None  # "1" - Player's win, "0.5" - draw, "0" - Lose
    ResultWin: Optional[int] = None
    ResultDraw: Optional[int] = None
    ResultLose: Optional[int] = None
    UTCDateTime: Optional[datetime] = None
    PlayerElo: Optional[int] = None
    OpponentElo: Optional[int] = None
    PlayerRatingChange: Optional[float] = None
    OpponentRatingChange: Optional[float] = None

    Variant: Optional[str] = None
    TimeControl: Optional[str] = None
    TimeControlSec: Optional[str] = None
    TimeControlType: Optional[str] = None

    ECO: Optional[str] = None
    OpeningFamily: Optional[str] = None
    OpeningVariation: Optional[str] = None
    OpeningSubVariation: Optional[str] = None

    PlayerTitle: Optional[str] = None
    OpponentTitle: Optional[str] = None
    Termination: Optional[str] = None

    @classmethod
    def from_pgn_header_std(
        cls, pgn_header_std: PGNGameHeaderStandardized, player_name: str
    ):
        outp = PGNGameHeaderPersonified()
        if player_name not in [pgn_header_std.White, pgn_header_std.Black]:
            raise ValueError(f"Player {player_name} is not in the game")

        if player_name == pgn_header_std.White:
            outp.Result = pgn_header_std.Result
            outp.Player = pgn_header_std.White
            outp.Opponent = pgn_header_std.Black
            outp.Color = "White"
            outp.PlayerElo = pgn_header_std.WhiteElo
            outp.OpponentElo = pgn_header_std.BlackElo
            outp.PlayerRatingChange = pgn_header_std.WhiteRatingChange
            outp.OpponentRatingChange = pgn_header_std.BlackRatingChange
            outp.PlayerTitle = pgn_header_std.WhiteTitle
            outp.OpponentTitle = pgn_header_std.BlackTitle
        else:
            outp.Result = 1 - pgn_header_std.Result
            outp.Player = pgn_header_std.Black
            outp.Opponent = pgn_header_std.White
            outp.Color = "Black"
            outp.PlayerElo = pgn_header_std.BlackElo
            outp.OpponentElo = pgn_header_std.WhiteElo
            outp.PlayerRatingChange = pgn_header_std.BlackRatingChange
            outp.OpponentRatingChange = pgn_header_std.WhiteRatingChange
            outp.PlayerTitle = pgn_header_std.BlackTitle
            outp.OpponentTitle = pgn_header_std.WhiteTitle

        outp.ResultWin = 1 if outp.Result == 1 else 0
        outp.ResultDraw = 1 if outp.Result == 0.5 else 0
        outp.ResultLose = 1 if outp.Result == 0 else 0

        outp.Event = pgn_header_std.Event
        outp.Site = pgn_header_std.Site
        outp.Date = pgn_header_std.Date
        outp.Round = pgn_header_std.Round
        outp.UTCDateTime = pgn_header_std.UTCDateTime
        outp.Variant = pgn_header_std.Variant
        outp.TimeControl = pgn_header_std.TimeControl
        outp.TimeControlSec = pgn_header_std.TimeControlSec
        outp.TimeControlType = pgn_header_std.TimeControlType
        outp.ECO = pgn_header_std.ECO
        outp.OpeningFamily = pgn_header_std.OpeningFamily
        outp.OpeningVariation = pgn_header_std.OpeningVariation
        outp.OpeningSubVariation = pgn_header_std.OpeningSubVariation
        outp.Termination = pgn_header_std.Termination

        return outp


# useful methods for data processing pipeline
# download games from API -> transform -> save to csv
class PipelineHelper:
    @staticmethod
    def write_to_csv(
        game_headers: list[PGNGameHeaderPersonified], file_path: str
    ) -> None:
        # Extract headers from PGNGameHeader fields
        header_fields = [field.name for field in fields(PGNGameHeaderPersonified)]

        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header_fields)

            # Write the headers to the first row
            writer.writeheader()

            # Write each game header as a row in the CSV
            for game_header in game_headers:
                writer.writerow(
                    {field: getattr(game_header, field) for field in header_fields}
                )

    @staticmethod
    def write_to_parquet(
        game_headers: list[PGNGameHeaderPersonified], file_path: str
    ) -> None:
        data = [asdict(game) for game in game_headers]
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
