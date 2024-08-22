from typing import Callable
from enum import Enum, auto
import logging
from pathlib import Path
import pandas as pd
import geopandas as gpd
import click
from returns.result import Success, Failure
from sqlalchemy import Engine
from sqlalchemy.exc import ProgrammingError
from datetime import datetime
from dateutil.parser import parser as date_parser, ParserError

from metadata.capture import RegistrationHandler


class LoadFileType(Enum):
    GEOJSON = auto()
    CSV = auto()
    PARQUET = auto()


class StopThePresses(Exception):
    """
    Raise this exception to stop execution before anything gets pushed 
    to the db.
    """


def build_workflow(
    config: dict, 
    destination_table: str, 
    working_dir: Path,
    cleanup_function: Callable[[gpd.GeoDataFrame | pd.DataFrame], gpd.GeoDataFrame | pd.DataFrame], 
    db_engine: Engine,
    filetype: LoadFileType = LoadFileType.GEOJSON
):

    @click.command()
    @click.argument("file_name")
    @click.argument("start_date")
    @click.option("-s", "--skip-metadata", is_flag=True, help="Skip metadata intake process")
    @click.option("-m", "--metadata-only", is_flag=True, help="Skip file load-in and only collect metadata")
    def workflow(file_name: str, start_date: str, skip_metadata: bool, metadata_only: bool):
        logger = logging.getLogger(config["app"]["name"])

        # Check if this is a new dataset, and start prompt workflow if so.
        logger.info(f"Opening {file_name}")
        try:
            match filetype:
                case LoadFileType.CSV:
                    raw_file = pd.read_csv(working_dir / "raw" / file_name)

                case LoadFileType.GEOJSON:
                    raw_file = gpd.read_file(working_dir / "raw" / file_name)

                case LoadFileType.PARQUET:
                    raw_file = pd.read_parquet(working_dir / "raw" / file_name)

        except FileNotFoundError:
            logger.error(
                f"The file you're trying to open, {file_name} doesn't exist"
            )
            return Failure(f"{file_name}, doesn't exist")

        # If the file is a new dataset, prompt for descriptions for every column
        # Check also if datatype that is reported by pandas is good.

        logger.info(f"Starting cleaning process.")
        try:
            cleaned = cleanup_function(raw_file)
        except StopThePresses:
            logger.info("'StopThePresses' was raised, so the presses were stopped.")
            return Failure(f"The presses were stopped.")

        # Add a source table
        # Add source information to the database

        if not metadata_only:
            iso_today = datetime.today().isoformat().split("T")[0]

            try:
                cleaned["edition"] = date_parser().parse(start_date)
                cleaned.to_parquet(
                    working_dir
                    / "backups"
                    / f"{destination_table}_{iso_today}.parquet.gzip"
                )
                match filetype:
                    case LoadFileType.CSV | LoadFileType.PARQUET:
                        cleaned.to_sql(
                            destination_table,
                            db_engine,
                            schema=config["app"]["name"],
                            if_exists="append",
                        )
                    case LoadFileType.GEOJSON:
                        cleaned.to_postgis(
                            destination_table,
                            db_engine,
                            schema=config["app"]["name"],
                            if_exists="append",
                        )

            except (ProgrammingError, ParserError) as e:
                match e:
                    case ProgrammingError():
                        logger.error(e)
                        logger.error("Error pushing completed file to the database, exiting.")
                        return Failure(
                            "Unable to push cleaned file to the database, check the logs."
                        )
                    case ParserError():
                        logger.error(e)
                        logger.error(f"Edition start date provided {start_date}")
                        return Failure("Bad start date.")

            logger.info(f"{file_name} was pushed to the database {start_date}")

        if not skip_metadata:
            logger.info(f"Starting metadata logging process.")
            meta_handler = RegistrationHandler(file_name, cleaned, config)
            meta_handler.run_complete_workflow()
        else:
            logger.info(f"Skip metadata selected, so no metadata will be logged.")

        return Success("Loading workflow complete!")

    return workflow
