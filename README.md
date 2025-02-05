
# Extract Player's game data for analysis

## Extracting Games Using the Lichess API

This section explains how to retrieve a list of a user's games in CSV format using the Lichess API, enabling further analysis.

1. **Obtain an API Token**  
To interact with the Lichess API, you need to generate a personal access token. You can do this at: [Lichess OAuth Token](https://lichess.org/account/oauth/token) and it's free :)

2. **Configure the Environment File**  
- Rename the file .env_default to .env.
- Open .env and add your Lichess API token.

3. **Install Python packages**

```bash
pip install -r requirements.txt
```

4. **Run the Script to Export Games**  
Use the `export_games_to_csv.py` script to fetch and save games in CSV format. The output file will be located at:
`output/games.csv`.
 

Example Command:

```bash
python export_games.py --username masslove --start-date 2024-01-01 --end-date 2024-12-31
```
Replace parameter values with your own.

For the full list of parameters (such as filtering by "blitz"/"bullet" type only, etc.) - execute:

```bash
python export_games.py -h
```

## How to analyze downloaded games

There are many ways to analyze downloaded file:

1. Load CSV files in Microsoft Excel and, then, use Pivot tables for analysis
2. Use BI tools like PowerBI desktop (which you can run for free on your computer) and load CSV file as a data source
3. Use Jupyter notebook - see an example `csv_analysis.ipynb`
4. Use query engines. One example could be AWS Athena.
