import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import http.client
import json
import os
from dotenv import load_dotenv
from urllib.parse import quote
from datetime import datetime
import pyarrow as pa
import fastparquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery


parquet_schema = pa.schema([
    ("team_key", pa.string()),
    ("team_name", pa.string()),
    ("team_country", pa.string()),
    ("team_founded", pa.string())
    # ("team_badge", pa.string()),
    # ("venue_name", pa.string()),
    # ("venue_address", pa.string()),
    # ("venue_city", pa.string()),
    # ("venue_capacity", pa.string()),
    # ("venue_surface", pa.string())
    #("players", pa.string())
])

bq_schema = {
    "fields": [
        {"name": "team_key", "type": "STRING", "mode": "NULLABLE"},
        {"name": "team_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "team_country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "team_founded", "type": "STRING", "mode": "NULLABLE"},
        # {"name": "team_badge", "type": "STRING", "mode": "NULLABLE"},
        # {"name": "venue_name", "type": "STRING", "mode": "NULLABLE"},
        # {"name": "venue_address", "type": "STRING", "mode": "NULLABLE"},
        # {"name": "venue_city", "type": "STRING", "mode": "NULLABLE"},
        # {"name": "venue_capacity", "type": "STRING", "mode": "NULLABLE"},
        # {"name": "venue_surface", "type": "STRING", "mode": "NULLABLE"},
        # {"name": "players", "type": "STRING", "mode": "NULLABLE"}
    ]
}

standing_parquet_schema = pa.schema([
    ('league_id', pa.string()),
    ('team_id', pa.string()),
    ('overall_league_position', pa.string()),
])


standings_bq_schema = {
    "fields": [
        {"name": "league_id", "type": "STRING"},
        {"name": "team_id", "type": "STRING"},
        {"name": "overall_league_position", "type": "STRING"},
    ]
}



# Load API key
load_dotenv()
API_KEY_2 = os.getenv("API_KEY_2")

class FetchTeamsInLeague(beam.DoFn): # possible to add FetchLeagueID and then take league_id as input
    def process(self, _):
        conn = http.client.HTTPSConnection("apiv3.apifootball.com")
        headers = {"x-apisports-key": API_KEY_2}
        query = quote("Premier League")
        conn.request("GET", f"/?action=get_teams&league_id=152&APIkey={API_KEY_2}", headers=headers)
        res = conn.getresponse()
        data = json.loads(res.read())
        # print(data)
        for team in data:
            if team is None:
                continue
            yield team

class FlattenTeamData(beam.DoFn):
    def process(self, team):
        if not isinstance(team, dict):
            return  # skip invalid rows
        yield {
            "team_key": str(team.get("team_key", "")),
            "team_name": str(team.get("team_name", "")),
            "team_country": str(team.get("team_country", "")),
            "team_founded": str(team.get("team_founded", ""))
        }

class FetchLeagueStandings(beam.DoFn):
    def process(self, league_id):
        conn = http.client.HTTPSConnection("apiv3.apifootball.com")
        headers = {"x-apisports-key": API_KEY_2}

        conn.request(
            "GET",
            f"/?action=get_standings&league_id={league_id}&APIkey={API_KEY_2}",
            headers=headers
        )
        
        res = conn.getresponse()
        data = json.loads(res.read())
        # print(data)
        for standing in data:
            if standing is None:
                continue
            yield standing


class FlattenStandingsData(beam.DoFn):
    def process(self, team_data):
        """
        team_data: dict representing a single team's standings info
        """
        # Validate the expected keys exist
        if not isinstance(team_data, dict):
            return  # skip invalid elements

        try:
            yield {
                "league_id": str(team_data.get("league_id", "")),
                "team_id": str(team_data.get("team_id", "")),
                "overall_league_position": str(team_data.get("overall_league_position", ""))
            }
        except KeyError as e:
            # log missing key but continue
            print(f"❌ MISSING KEY {e} in team data: {team_data}")



def run():
    # PROJECT_ID = os.getenv("PROJECT_ID")
    # REGION     = os.getenv("REGION")
    # BUCKET     = os.getenv("BUCKET")

    # options = PipelineOptions(
    #     runner="DataflowRunner",
    #     project=PROJECT_ID,
    #     region=REGION,
    #     temp_location=f"gs://{BUCKET}/temp"
    # )
    options = PipelineOptions(
        runner="DirectRunner",  # Or "DataflowRunner" if running on GCP
        temp_location="gs://sport__bucket/temp",  # Required for Dataflow
        project="voltaic-tooling-471807-t5",  # Your GCP project ID
        region="us-central1"
    )
    # Generate timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    with beam.Pipeline(options=options) as p:
        # Fetch and flatten teams
        teams = (
            p
            | "Start" >> beam.Create([None])
            | "Fetch Teams" >> beam.ParDo(FetchTeamsInLeague())
            | "Flatten Teams" >> beam.ParDo(FlattenTeamData())
            | "Filter None" >> beam.Filter(lambda x: x is not None)
        )

        # Write to Parquet
        teams | "Write Teams to Parquet" >> beam.io.parquetio.WriteToParquet(
            f"gs://sport__bucket/teams/API2/premier_league_{timestamp}",
            schema=parquet_schema,
            file_name_suffix=".parquet"
        )

        # Write to BigQuery
        teams | "Write Teams to BigQuery" >> WriteToBigQuery(
            table="voltaic-tooling-471807-t5.teams.teams_API2",
            project="voltaic-tooling-471807-t5",
            schema=bq_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        standings = (
            p
            | "Standings League ID" >> beam.Create([152])
            | "Fetch Standings" >> beam.ParDo(FetchLeagueStandings())
            | "Flatten Standings" >> beam.ParDo(FlattenStandingsData())
            | "Filter Standings None" >> beam.Filter(lambda x: x is not None)
        )


        # Write to Parquet
        standings | "Write Standings to Parquet" >> beam.io.parquetio.WriteToParquet(
            f"gs://sport__bucket/standings/API2/premier_league_{timestamp}",
            schema=parquet_schema,
            file_name_suffix=".parquet"
        )

        #write to BigQuery
        standings | "Write Standings to BigQuery" >> WriteToBigQuery(
            table="voltaic-tooling-471807-t5.teams.standings_API2",
            schema=standings_bq_schema,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED"
        )


if __name__ == "__main__":
    run()
