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

parquet_schema = pa.schema([
    ('rank', pa.int64()),
    ('team_id', pa.int64()),
    ('team_name', pa.string()),
    ('points', pa.int64()),
    ('goals_diff', pa.int64()),
    ('season', pa.int64()),
    ('league_id', pa.int64())
])
# Load API key
load_dotenv()
API_KEY_1 = os.getenv("API_KEY_1")

class FetchLeagueID(beam.DoFn):
    def process(self, _):
        conn = http.client.HTTPSConnection("v3.football.api-sports.io")
        headers = {"x-apisports-key": API_KEY_1}
        query = quote("Premier League")
        conn.request("GET", f"/leagues?name={query}&country=england", headers=headers)
        res = conn.getresponse()
        data = json.loads(res.read())
        print(data)
        for item in data.get("response", []):
            league = item.get("league", {})
            country = item.get("country", {})
            if league.get("name") == "Premier League" and country.get("name") == "England":
                yield league.get("id")  # yield exact league ID

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
        (
            p
            | "Start" >> beam.Create([None])
            | "Fetch League ID" >> beam.ParDo(FetchLeagueID())
            | "Fetch Standings" >> beam.ParDo(FetchLeagueStandings())

            |"Write to Parquet" >> beam.io.parquetio.WriteToParquet(
                f"gs://sport__bucket/standings/API1/premier_league_{timestamp}",
                schema=parquet_schema,
                file_name_suffix=".parquet"
            )
        )

if __name__ == "__main__":
    run()
