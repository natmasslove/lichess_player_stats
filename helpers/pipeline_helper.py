from datetime import datetime, timezone
from dataclasses import fields, dataclass
from typing import Optional

import csv

from helpers.lichess_api_helper import PGNGameHeader


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
    Round: Optional[str] = None

    White: Optional[str] = None
    Black: Optional[str] = None

    Result: Optional[float] = None  # "1" - White wins, "0.5" - draw, "0" - Black
    UTCDateTime: Optional[datetime] = None
    WhiteElo: Optional[int] = None
    BlackElo: Optional[int] = None
    WhiteRatingChange: Optional[int] = None
    BlackRatingChange: Optional[int] = None

    Variant: Optional[str] = None
    TimeControl: Optional[str] = None

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
    def parse_time_control(cls, time_control: str) -> str:
        time_control_minutes = None
        time_control_increment = None
        if time_control:
            try:
                initial, increment = map(int, time_control.split("+"))
                time_control_minutes = initial // 60
                time_control_increment = increment
                return f"{time_control_minutes}+{time_control_increment}"
            except ValueError:
                pass
        else:
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
    def from_pgn_header(cls, pgn_header: PGNGameHeader):
        outp = PGNGameHeaderStandardized()
        outp.Event = pgn_header.Event
        outp.Site = pgn_header.Site
        outp.Date = pgn_header.Date
        outp.Round = pgn_header.Round
        outp.White = pgn_header.White
        outp.Black = pgn_header.Black

        results_map = {"1-0": 1, "0-1": 0, "1/2-1/2": 0.5}
        outp.Result = results_map.get(pgn_header.Result)
        outp.UTCDateTime = cls.join_utc_date_and_time(
            pgn_header.UTCDate, pgn_header.UTCTime
        )

        outp.WhiteElo = cls.str_to_int(pgn_header.WhiteElo)
        outp.BlackElo = cls.str_to_int(pgn_header.BlackElo)

        outp.WhiteRatingChange = cls.str_to_int(pgn_header.WhiteRatingDiff)
        outp.BlackRatingChange = cls.str_to_int(pgn_header.BlackRatingDiff)

        outp.Variant = pgn_header.Variant
        outp.TimeControl = cls.parse_time_control(pgn_header.TimeControl)

        outp.ECO = pgn_header.ECO

        outp.OpeningFamily, outp.OpeningVariation, outp.OpeningSubVariation = (
            cls.split_opening(pgn_header.Opening)
        )

        return outp

        #################################
        # stopped here

        # Parse TimeControl field

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
