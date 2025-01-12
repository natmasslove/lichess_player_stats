from datetime import datetime
from dataclasses import fields, dataclass
from typing import Optional

import csv

from helpers.lichess_api_helper import PGNGameHeader
from django.utils import timezone


# useful methods for data processing pipeline
# download games from API -> transform -> save to csv
class PipelineHelper:
    @staticmethod
    def write_to_csv(game_headers: list[PGNGameHeader], file_path: str) -> None:
        # Extract headers from PGNGameHeader fields
        header_fields = [field.name for field in fields(PGNGameHeader)]

        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header_fields)

            # Write the headers to the first row
            writer.writeheader()

            # Write each game header as a row in the CSV
            for game_header in game_headers:
                writer.writerow(
                    {field: getattr(game_header, field) for field in header_fields}
                )


@dataclass
class PGNGameHeaderStandardized:
    Event: Optional[str] = None
    Site: Optional[str] = None
    Date: Optional[str] = None
    Round: Optional[int] = None

    White: Optional[str] = None
    Black: Optional[str] = None

    Result: Optional[float] = None  # "1" - White wins, "0.5" - draw, "0" - Black
    UTCDateTime: Optional[datetime] = None
    WhiteElo: Optional[int] = None
    BlackElo: Optional[int] = None
    WhiteRatingDiff: Optional[int] = None
    BlackRatingDiff: Optional[int] = None

    Variant: Optional[str] = None
    TimeControl: Optional[str] = None

    ECO: Optional[str] = None
    OpeningMain: Optional[str] = None
    OpeningSub: Optional[str] = None
    OpeningVariation: Optional[str] = None

    WhiteTitle: Optional[str] = None
    BlackTitle: Optional[str] = None
    Termination: Optional[str] = None

    @classmethod
    def from_pgn_header(cls, pgn_header: PGNGameHeader):
        results_map = {"1-0": 1, "0-1": 0, "1/2-1/2": 0.5}
        result = results_map.get(pgn_header.Result)

        # Parse UTC Date and UTC Time
        utc_datetime = None
        if pgn_header.UTCDate and pgn_header.UTCTime:
            try:
                date_str = f"{pgn_header.UTCDate} {pgn_header.UTCTime}"
                utc_datetime = datetime.strptime(date_str, "%Y.%m.%d %H:%M:%S")
                utc_datetime = utc_datetime.replace(tzinfo=timezone.utc)
            except ValueError:
                raise ValueError(
                    f"Invalid UTCDate {pgn_header.UTCDate} or UTCTime format {pgn_header.UTCTime}"
                )

        def str_to_int(value: Optional[str]) -> Optional[int]:
            if value is None or value.strip() == "":
                return None
            try:
                return int(value)
            except ValueError:
                return None

        white_elo = str_to_int(pgn_header.WhiteElo)
        black_elo = str_to_int(pgn_header.BlackElo)

        #################################
        # stopped here

        # Parse TimeControl field
        time_control_minutes = None
        time_control_increment = None
        if pgn_header.TimeControl:
            try:
                initial, increment = map(int, pgn_header.TimeControl.split("+"))
                time_control_minutes = initial // 60
                time_control_increment = increment
            except ValueError:
                pass

        # Parse Opening field into main, sub, and variation
        opening_main, opening_sub, opening_variation = None, None, None
        if pgn_header.Opening:
            parts = pgn_header.Opening.split(",")
            opening_main = parts[0].strip() if len(parts) > 0 else None
            opening_sub = parts[1].strip() if len(parts) > 1 else None
            opening_variation = parts[2].strip() if len(parts) > 2 else None

        # Return cleansed data
        return cls(
            Event=pgn_header.Event,
            Site=pgn_header.Site,
            Date=date,
            Round=round_number,
            White=pgn_header.White,
            Black=pgn_header.Black,
            Result=pgn_header.Result,
            UTCDate=None,
            Score=None,
            WhiteElo=white_elo,
            BlackElo=black_elo,
            WhiteRatingDiff=(
                int(pgn_header.WhiteRatingDiff)
                if pgn_header.WhiteRatingDiff and pgn_header.WhiteRatingDiff.isdigit()
                else None
            ),
            BlackRatingDiff=(
                int(pgn_header.BlackRatingDiff)
                if pgn_header.BlackRatingDiff and pgn_header.BlackRatingDiff.isdigit()
                else None
            ),
            Variant=pgn_header.Variant,
            TimeControlMinutes=time_control_minutes,
            TimeControlIncrement=time_control_increment,
            ECO=pgn_header.ECO,
            OpeningMain=opening_main,
            OpeningSub=opening_sub,
            OpeningVariation=opening_variation,
            WhiteTitle=pgn_header.WhiteTitle,
            BlackTitle=pgn_header.BlackTitle,
            Termination=pgn_header.Termination,
        )
