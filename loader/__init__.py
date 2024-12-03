from typing import Callable
from enum import Enum, auto
import logging
from pathlib import Path
import pandas as pd
import pandera as pa
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
    JSON = auto()


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
    filetype: LoadFileType = LoadFileType.GEOJSON,
    preload: Callable[[Path], pd.DataFrame] | None = None,
    mute_metadata: bool = False,
    delimiter: str | None = None,
    schema: pa.DataFrameSchema | None = None
):

    @click.command()
    @click.argument("file_name")
    @click.argument("start_date")
    @click.option("-s", "--skip-metadata", is_flag=True, help="Skip metadata intake process")
    @click.option("-m", "--metadata-only", is_flag=True, help="Skip file load-in and only collect metadata")
    @click.option("-ev", "--use-editor", is_flag=True, help="Use default editor to enter in descriptions")
    def workflow(file_name: str, start_date: str, skip_metadata: bool, metadata_only: bool, use_editor: bool):
        logger = logging.getLogger(config["app"]["name"])

        # Check if this is a new dataset, and start prompt workflow if so.
        logger.info(f"Opening {file_name}")

        filepath = working_dir / "raw" / file_name
        try:
            if preload:
                """
                A pre load function allows you to open the file however you want
                and return the dataframe -- this is useful for files that aren't 
                structured in a way that can be immediately opened with pandas.
                """
                raw_file = preload(filepath)

            else:
                match filetype:
                    case LoadFileType.CSV:
                        if delimiter:
                            raw_file = pd.read_csv(filepath, delimiter=delimiter)
                        else:
                            raw_file = pd.read_csv(filepath)

                    case LoadFileType.GEOJSON:
                        raw_file = gpd.read_file(filepath)

                    case LoadFileType.PARQUET:
                        raw_file = pd.read_parquet(filepath)

                    case LoadFileType.JSON:
                        raw_file = pd.read_json(filepath)

        except (FileNotFoundError, StopThePresses) as e:
            match e:
                case FileNotFoundError():
                    logger.error(
                        f"The file you're trying to open, {file_name} doesn't exist"
                    )
                    return Failure(f"{file_name}, doesn't exist")
                case StopThePresses():
                    logger.error("Presses were stopped in preload function.")
                    return Failure("Presses were stopped in preload function.")

        # If the file is a new dataset, prompt for descriptions for every column
        # Check also if datatype that is reported by pandas is good.

        logger.info(f"Starting cleaning process.")
        try:
            ### This is where the work from the dataset-specific file is ran
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
                    case LoadFileType.CSV | LoadFileType.PARQUET | LoadFileType.JSON:
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

        if not (skip_metadata or mute_metadata):
            logger.info(f"Starting metadata logging process.")
            meta_handler = RegistrationHandler(file_name, cleaned, config, vim_edit=use_editor)
            meta_handler.run_complete_workflow()
        else:
            logger.info(f"Skip metadata selected, so no metadata will be logged.")

        return Success("Loading workflow complete!")

    return workflow
