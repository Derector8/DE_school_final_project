import logging
import multiprocessing

import pandas as pd
from rdkit import rdBase
from rdkit.Chem import (
    MolFromSmiles,
    AllChem,
    Mol,
     )

import postgres_connector
import s3_connector
from columns_data import fps_dtype_dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ChemBL_Fingerprints_Compute_Logger")

fps_postgres = postgres_connector.PostgresConnector("molecules_fingerprints", fps_dtype_dict)
fps_s3 = s3_connector.S3Connector()

rdBase.DisableLog("rdApp.warning")  # Added to hide Deprecation Warnings of GetMorganFingerprintAsBitVect
pd.options.mode.copy_on_write = True


class MorganFingerprintsCalculator:
    """
    A Class to represent Morgan fingerprints calculator object for ChemBL molecules ingested in Postgres.
    Include methods to check smiles and compute Morgan fingerprints with multiprocessing and without.

    """
    FPS_BITS = 2048
    FPS_MOL_RADIUS = 2

    COMPUTE_NUM_PROCESSES = 4

    def __init__(self, chunksize: int = 100000, s3_fps_folder: str = "final_task/bukharev_roman/chembl_fps"):

        self.chunksize = chunksize,
        self.s3_fps_folder = s3_fps_folder
        self.df_chunks = None

    @staticmethod
    def __parse_smiles(smiles: str) -> "Mol | None":
        """ Try to parse smiles and return None in case something wrong with it """
        try:
            return MolFromSmiles(smiles)
        except:
            return None

    @classmethod
    def _calculate_fps_chunk(cls, chunk_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Morgan fingerprints for provided dataframe"""
        chunk_df['mol'] = chunk_df['canonical_smiles'].apply(cls.__parse_smiles)

        chunk_df = chunk_df.loc[chunk_df['mol'].notna()]

        chunk_df['fps'] = chunk_df['mol'].apply(
            lambda mol: AllChem.GetMorganFingerprintAsBitVect(mol, cls.FPS_MOL_RADIUS, nBits=cls.FPS_BITS).ToBitString()
        )
        chunk_df.drop(columns=['mol', 'canonical_smiles'], inplace=True)
        return chunk_df

    def calculate_fps(self, parallelize: bool) -> "pd.DataFrame | list":
        """Compute Morgan fingerprints for list of dataframes - self.df_chunks"""
        chunks = self.df_chunks

        if parallelize:
            pool = multiprocessing.Pool(processes=self.COMPUTE_NUM_PROCESSES)
            res_iter = pool.imap(self._calculate_fps_chunk, chunks)
        else:
            res_iter = map(self._calculate_fps_chunk, chunks)

        fps_list = list(res_iter)

        return fps_list


def run():
    mfc = MorganFingerprintsCalculator()
    mfc.chunks = fps_postgres.load_data_from_postgres_to_df_chunks(chunk_size=mfc.chunksize)

    fps_list = mfc.calculate_fps(parallelize=True)
    offset = 0
    for chunk in fps_list:
        fps_postgres.save_df_to_postgres(chunk)
        fps_s3.save_parquet_to_s3(chunk, file_name=f"ChemBL_fingerprints_chunk({offset}-{offset + mfc.chunksize})")
        offset += mfc.chunksize
        logger.info("One cycle successfully ended")

    logger.info("All cycle successfully ended")
