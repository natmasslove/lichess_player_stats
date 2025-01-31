import os
import json
import dotenv
import argparse
from datetime import datetime, timezone
from helpers.lichess_api_helper import (
    LichessAPIHelper,
    APIParams_GetGames,
    PGNGameHeader,
    ChessPerfType,
)
from helpers.pipeline_helper import (
    PipelineHelper,
    PGNGameHeaderStandardized,
    PGNGameHeaderPersonified,
)

dotenv.load_dotenv()
lichess_api_token = os.environ["LICHESS_TOKEN"]

if __name__ == "__main__":
    # Parse parameters
    parser = argparse.ArgumentParser(
        description="Generate timestamps for a given date range."
    )
    parser.add_argument(
        "--username",
        required=True,
        help="Lichess user whose games are exported",
    )
    parser.add_argument(
        "--perf-type",
        required=False,
        default=None,
        help=f"Comma-separated games type to export. Valid values: {', '.join(variant.value for variant in ChessPerfType)}",
    )
    parser.add_argument(
        "--max-games",
        required=False,
        default=None,
        type=int,
        help="Maximum number of games to export",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format",
    )

    args = parser.parse_args()
    username = args.username
    max_games = args.max_games
    try:
        perf_types = LichessAPIHelper.validate_perf_types(args.perf_type)
    except ValueError as e:
        print(f"Error: {e}")
        exit(1)

    start_ts, end_ts = LichessAPIHelper.generate_timestamps_msec(
        args.start_date, args.end_date
    )

    print(f"Exporting games of {username}")
    print(f"Period: ({args.start_date} 00:00:00) - ({args.end_date} 23:59:59)")
    print(f"Type(s): {args.perf_type}")
    print("Note that download limit is 30-60 games per second, so be patient... :)")

    api_helper = LichessAPIHelper(lichess_api_token)

    # Prepare params for Lichess API call
    params = APIParams_GetGames(
        max=max_games,
        rated=True,
        perfType=perf_types,
        moves=False,
        opening=True,
        since=start_ts,
        until=end_ts,
    )

    # Get games as-is from lichess API
    pgn_data_raw: list[PGNGameHeader] = api_helper.get_games_headers(username, params)

    # 'Standardize' - type conversion, normalization (like time control conversion from 180+0 to 3+0)
    pgn_data_std: list[PGNGameHeaderStandardized] = [
        PGNGameHeaderStandardized.from_pgn_header(item) for item in pgn_data_raw
    ]

    # Instead of generic game Info with White and Black, we converting to Player's (the one we exporting for) perspective
    pgn_data_pers: list[PGNGameHeaderPersonified] = [
        PGNGameHeaderPersonified.from_pgn_header_std(item, username)
        for item in pgn_data_std
    ]

    # write raw data from API
    output_folder = "output"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_filename = os.path.join(output_folder, "games.csv")
    PipelineHelper.write_to_csv(pgn_data_pers, output_filename)

    print(f"Done! {len(pgn_data_raw)} games have been saved to '{output_filename}' ")
