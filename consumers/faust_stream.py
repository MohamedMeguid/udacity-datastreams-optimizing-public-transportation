"""Defines trends calculations for stations"""
import json
import logging

import faust

from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

    def serialize(self):
        return asdict(self)


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("org.chicago.cta.tables.stations", value_type=Station)
# Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

# Define a Faust Table
table = app.Table(
   "org.chicago.cta.stations.table.v1",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


# Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`

def map_color_to_line(station: Station):
    station.line = ""
    if station.red:
        station.line = "red"
    elif station.blue:
        station.line = "blue"
    elif station.green:
        station.line = "green"
    return station

@app.agent(topic)
async def transform_station(stations):
    
    stations.add_processor(map_color_to_line)

    async for station in stations:
        logger.info(f"Processing station id:{station.station_id}, name:{station.station_name}, order:{station.order}, line:{station.line}")
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=station.line
        )
        table[transformed_station.station_id] = transformed_station
        logger.info(f"Table entry: {table[transformed_station.station_id]}")


if __name__ == "__main__":
    app.main()
