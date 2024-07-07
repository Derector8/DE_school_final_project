from multiprocessing import set_start_method
set_start_method("spawn", force=True)  # for processes not to stuck if db closes connection abnormally.

import logging
import time
import multiprocessing

import pandas as pd
import requests
from psycopg2._json import Json
from psycopg2.extensions import register_adapter

from columns_data import (
    raw_molecule_cols,
    raw_chembl_id_lookup_cols,
    raw_molecule_dtype_dict,
    raw_chembl_id_lookup_dtype_dict,
)
import postgres_connector


class DatabaseIngestionFromApiBaseException(Exception):
    """ Base Exception for all exceptions in database ingestion from API script """
    pass


class MaxTriesLimitError(DatabaseIngestionFromApiBaseException):
    """ General Exception for all errors with exceeding the attempts limit"""
    pass


class SavingMaxTriesLimitError(MaxTriesLimitError):
    pass


class CreatingChunkMaxTriesLimitError(MaxTriesLimitError):
    pass


class CountTotalRowsMaxTriesLimitError(MaxTriesLimitError):
    pass


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ChemBL_Database_Ingestion_Logger")

register_adapter(dict, Json)  # Register adapter for postgres to convert dicts to Json


class DatabaseIngestionFromApi:
    """
    A Class to represent Database Ingestion object.
    Include methods to load ChemBL data from API and put it in staging tables

    Attributes
    ----------
    mode : str
        a
    partition : str
        the
    max_tries : str
        the
    num_of_proc : int
        the
    table_name : str
        a
    path : str
        the
    cols : str
        the
    dtype_dict : int
        the
    """
    API_DATA_URL = "https://www.ebi.ac.uk/chembl/api/data/"

    def __init__(self, mode: str, partition: int = 1000, max_tries: int = 10, num_of_proc: int = 4):

        self.mode = mode
        self.partition = partition
        self.max_tries = max_tries
        self.num_of_proc = num_of_proc
        self.table_name = f"staging_{mode}"
        self.path = f"{mode}.json?limit={partition}&offset="
        if mode == "molecule":
            self.cols = raw_molecule_cols
            self.dtype_dict = raw_molecule_dtype_dict
        elif mode == "chembl_id_lookup":
            self.cols = raw_chembl_id_lookup_cols
            self.dtype_dict = raw_chembl_id_lookup_dtype_dict

    def chembl_molecules_api_call(self, offset: int) -> "requests.Response | None":
        """ Try to fetch and return data from API url, else return None"""
        try:
            response = requests.get(f"{DatabaseIngestionFromApi.API_DATA_URL}{self.path}{offset}")
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            logger.error(f"{e}")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"{e}")
            return None

        logger.debug("Data from API fetched successfully")

        return response

    def total_rows_count(self) -> bool:
        """
        Count total rows information from API response meta data.
        Returns True if success and False if fail.
        """
        response = self.chembl_molecules_api_call(offset=0)
        if not response:
            return False
        try:
            data = response.json()
        except AttributeError as e:
            logger.error(f"{e}")
            return False

        self.total_rows = data["page_meta"]["total_count"]

        return True

    def molecules_to_df(self, response: requests.Response) -> "pd.DataFrame | None":
        """
        Creates dataframe from API response
        Returns Dataframe or None in case of error
        """
        try:
            data = response.json()
        except AttributeError as e:
            logger.error(f"{e}")
            return None

        logger.debug("Trying to create molecules df from response...")

        df = pd.json_normalize(data[f"{self.mode}s"], max_level=0)
        df = df[self.cols]

        logger.debug("Df created")

        return df

    def create_bigger_chunks(self, first_offset: int, last_offset: int, multiplier: int = 10) -> pd.DataFrame:
        """Creates dataframe from multiple API responses(according to multiplier)"""
        df_list = []
        max_tries = self.max_tries

        offset = first_offset
        max_offset = offset + self.partition * multiplier
        while offset < max_offset:
            if max_tries <= 0:
                raise CreatingChunkMaxTriesLimitError

            response = self.chembl_molecules_api_call(offset=offset)

            if response:
                df = self.molecules_to_df(response)
            else:
                time.sleep(30)
                logger.info(f"Trying one more time...There are{max_tries} attempts of {self.max_tries} left")
                max_tries -= 1
                continue

            df_list.append(df)

            if offset < last_offset - self.partition:
                offset += self.partition
                logger.debug(f"{offset} rows in big chunk ")
            else:
                break

        big_df = pd.concat(df_list, axis=0)

        return big_df

    def ingestion_process(self, first_offset: int, last_offset: int, multiplier: int = 10) -> None:
        """
        Full database ingestion process - loading all ChemBL molecules from first_offset to last_offset pages
        from API to Postgres
        """
        postgres = postgres_connector.PostgresConnector(self.table_name, self.dtype_dict)

        max_tries = self.max_tries
        start = time.time()
        last_rows_time = start
        offset = first_offset

        while offset < last_offset:
            logger.info(f"Now requesting {offset} - {offset + self.partition * multiplier} rows...")
            df = self.create_bigger_chunks(offset, last_offset, multiplier=multiplier)

            save_success = postgres.save_df_to_postgres(df)

            if not save_success:
                if max_tries <= 0:
                    raise SavingMaxTriesLimitError
                max_tries -= 1
                logger.error(f"Data not saved to db. Trying one more time...")
                logger.info(f"Fetching rows {offset} - {offset + self.partition * multiplier} one more time")
                continue

            logger.info(f"Rows {offset} - {offset + self.partition * multiplier} inserted!")

            new_rows_time = time.time()
            logger.info(f"time from start: {new_rows_time - start}")
            logger.info(f"time this insertation required: {new_rows_time - last_rows_time}")
            last_rows_time = new_rows_time

            max_tries = self.max_tries
            offset += self.partition * multiplier

    def ingestion_multiprocess(self, multiplier: int = 10) -> None:
        """Creates processes for multiprocessing database ingestion"""
        max_tries = self.max_tries
        while max_tries > 0:
            total_rows = self.total_rows_count()
            if total_rows:
                logger.info(f"{self.mode} data have {self.total_rows} rows")
                break
            else:
                logger.error(f"Trying to fetch total number of rows one more time...")
                max_tries -= 1
                time.sleep(30)
        if max_tries <= 0:
            raise CountTotalRowsMaxTriesLimitError

        rows_per_proc = self.total_rows // self.num_of_proc // self.partition
        jobs = []
        for i in range(self.num_of_proc):
            first_offset = i * rows_per_proc * self.partition
            last_offset = first_offset + rows_per_proc * self.partition
            if i == self.num_of_proc - 1:
                last_offset = self.total_rows

            p = multiprocessing.Process(target=self.ingestion_process,
                                        args=(first_offset, last_offset, multiplier))
            jobs.append(p)
            p.start()
            time.sleep(60)

        for proc in jobs:
            proc.join()


def run(mode: str = "molecule", max_tries: int = 10, multiplier: int = 10) -> None:
    """
    Ingest molecules or chembl_id_lookups database from ChemBL API to Postgres database.
    
    Parameters
        ----------
        mode : str
            Choose database to ingest: 'molecule' or 'chembl_id_lookup'
        max_tries : int
            Set the limit of repeated attempts if errors occured durin process
        multiplier : int
            Set number of pages/responses will be loaded to Postgres in one transaction
    """
    difa = DatabaseIngestionFromApi(
        mode=mode,
        max_tries=max_tries,
    )
    total_rows = difa.total_rows_count()
    if total_rows:
        difa.ingestion_process(first_offset=0, last_offset=total_rows, multiplier=multiplier)


def run_multiprocessor(mode="molecule", max_tries: int = 30, multiplier: int = 10, num_of_proc: int = 4) -> None:
    """
    Ingest molecules or chembl_id_lookups database from ChemBL API to Postgres database
    with a set number of processes in multiprocessing style.
    
    Parameters
        ----------
        mode : str
            Choose database to ingest: 'molecule' or 'chembl_id_lookup'
        max_tries : int
            Set the limit of repeated attempts if errors occured durin process
        multiplier : int
            Set number of pages/responses will be loaded to Postgres in one transaction
        num_of_proc : int
            Set number of processors, which you want to use
    """
    difa = DatabaseIngestionFromApi(
        mode=mode,
        max_tries=max_tries,
        num_of_proc=num_of_proc,
    )
    logger.debug("Trying to start multiproc")
    difa.ingestion_multiprocess(multiplier=multiplier)
