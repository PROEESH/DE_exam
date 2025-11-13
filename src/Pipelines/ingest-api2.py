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
        print(data)
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
            # "team_badge": str(team.get("team_badge", "")),
            # "venue_name": str(team.get("venue_name", "")),
            # "venue_address": str(team.get("venue_address", "")),
            # "venue_city": str(team.get("venue_city", "")),
            # "venue_capacity": str(team.get("venue_capacity", "")),
            # "venue_surface": str(team.get("venue_surface", "")),
            #"players": json.dumps(players)  # store list as JSON string
        }
class FetchLeagueStandings(beam.DoFn):
    def process(self, league_id):
        conn = http.client.HTTPSConnection("v3.football.api-sports.io")
        headers = {"x-apisports-key": API_KEY_1}
        season = 2022
        #page = 1
        conn.request(
            "GET",
            f"/standings?league={league_id}&season={season}", #&page={page}
            headers=headers
        )
        res = conn.getresponse()
        data = json.loads(res.read())

        print(data)


def run():
    options = PipelineOptions(
        runner="DirectRunner",  # Or "DataflowRunner" if running on GCP
        temp_location="gs://sport__bucket",  # Required for Dataflow
        project="voltaic-tooling-471807-t5"  # Your GCP project ID
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
        teams | "Write to Parquet" >> beam.io.parquetio.WriteToParquet(
            f"gs://sport__bucket/teams/API2/premier_league_{timestamp}",
            schema=parquet_schema,
            file_name_suffix=".parquet"
        )

        # Write to BigQuery
        teams | "Write to BigQuery" >> WriteToBigQuery(
            table="voltaic-tooling-471807-t5.teams.teams",
            project="voltaic-tooling-471807-t5",
            schema=bq_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )



if __name__ == "__main__":
    run()
