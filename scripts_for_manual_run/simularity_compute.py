import logging
import time

import numpy as np
import pandas as pd
from rdkit import rdBase
from rdkit.DataStructs import (
    CreateFromBitString,
    ExplicitBitVect,
)
from rdkit.DataStructs import TanimotoSimilarity
from sqlalchemy import (
    text as sql_text,
)

import postgres_connector
import s3_connector
from columns_data import fact_sim_dtype_dict

rdBase.DisableLog("rdApp.warning")  # Added to hide Deprecation Warnings of GetMorganFingerprintAsBitVect
pd.options.mode.copy_on_write = True

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ChemBL_Similarity_Compute_Logger")

sim_postgres = postgres_connector.PostgresConnector("fact_top10_molecule_similarities", fact_sim_dtype_dict)
sim_s3 = s3_connector.S3Connector()


class SimilarityCalculator:
    """
    A Class to represent Tanimoto similarity calculator object for input data, stored in s3 bucket.
    Include methods to compute Tanimoto similarity of input data molecules with all molecules from ChemBL database

    """
    S3_SIM_FOLDER = "final_task/bukharev_roman/chembl_similarities"
    S3_FPS_FOLDER = "final_task/bukharev_roman/chembl_fps"

    def __init__(self):

        pass

    @classmethod
    def _compute_tanimoto_similarity(cls, fps_df: pd.DataFrame, target_fps: ExplicitBitVect) -> pd.DataFrame:
        """Computes Tanimoto similarity for target fingerprints with all other molecules in fps_df"""
        fps_df['tanimoto_similarity_score'] = fps_df['fps'].apply(
            lambda fps: TanimotoSimilarity(target_fps, fps)
        )
        return fps_df

    @classmethod
    def _prepare_similarity_df(cls, sim_df: pd.DataFrame, target_name: str) -> pd.DataFrame:
        """
        Prepares dataframe with target molecule similarities:
        Adds target molecule chembl_id column, set column names, deletes same molecules similarities and order columns.
        """
        sim_df = sim_df[['chembl_id', 'tanimoto_similarity_score']].copy()
        sim_df["target_chembl_id"] = target_name
        sim_df.rename(columns={'chembl_id': 'source_chembl_id'}, inplace=True)
        sim_df = sim_df[sim_df.source_chembl_id != target_name]
        sim_df = sim_df[["target_chembl_id", 'source_chembl_id', 'tanimoto_similarity_score']]

        return sim_df

    @classmethod
    def create_full_similarity_df(cls, fps_df: pd.DataFrame, target_name: str,
                                  target_fps: ExplicitBitVect) -> pd.DataFrame:
        """Creates full similarity dataframe for target molecule"""
        raw_sim_df = cls._compute_tanimoto_similarity(fps_df, target_fps)
        sim_df = cls._prepare_similarity_df(raw_sim_df, target_name)

        return sim_df

    @classmethod
    def fast_validate_s3_data(cls, csv_data_df: pd.DataFrame) -> dict:
        """Checks for correctness input molecules names,smiles and creates a dict with valid names/smiles"""
        csv_data_df.drop_duplicates(subset=["molecule name"], inplace=True)
        csv_data_df.drop_duplicates(subset=["smiles"], inplace=True)
        csv_dict = {}
        id_list = []
        smiles_list = []

        for index, row in csv_data_df.iterrows():
            id_list.append(row['molecule name'])
            smiles_list.append(row['smiles'])

        sql = f"SELECT srf.chembl_id, srf.fps, cs.canonical_smiles FROM staging_raw_fingerprints srf join compound_structures cs on srf.chembl_id = cs.chembl_id WHERE (srf.chembl_id in {tuple(id_list)}) or (cs.canonical_smiles in {tuple(smiles_list)});"

        res_df = sim_postgres.load_data_from_postgres_to_df(sql_text(sql))
        for index, row in res_df.iterrows():
            csv_dict[row["chembl_id"]] = row["fps"]
            print(f"{row['chembl_id']} added to dict")

        logger.debug(f"csv_dict: {csv_dict}")
        logger.debug(f"Return type: {type(csv_dict)}")

        return csv_dict


def compute_similarity(file_name: str = "2024") -> None:
    # ToDo Split this big function to smaller ones
    """
    Computes full and top 10 similarities for input file's molecules.
    Saves full similarity to s3 bucket in parquet file.
    Ingests top 10 similarities in Postgres fact table.
    If many files have file_name parameter in name - load all and puts in Dataframe
    
    Parameters
        ----------
        file_name : str
            Template for input file/files
    """
    sc = SimilarityCalculator()
    files_list = sim_s3.list_files_in_folder(sim_s3.INPUT_FOLDER_NAME,
                                             file_name)  # Get list of suitable files in s3 folder
    logger.debug(f"{files_list}")

    csv_df = sim_s3.load_input_csv_from_s3_to_df(files_list)  # Creates df with all input molecules
    logger.debug(f"Csv file loaded in dataframe")
    csv_dict = sc.fast_validate_s3_data(csv_df)  # Checks for correctness input molecules names and smiles

    parquet_list = sim_s3.list_files_in_folder(sc.S3_FPS_FOLDER,
                                               ".parquet")  # Finds all computed fingerprints in s3 bucket
    fps_df = sim_s3.load_parquet_from_s3(parquet_list)  # Loads all computed fingerprints from s3 bucket
    logger.debug(f"Parquet fingerprints files loaded in dataframe")

    fps_df['fps'] = fps_df['fps'].map(CreateFromBitString)  # Converts fps strings in ExplicitBitVect objects

    start = time.time()
    for key, value in csv_dict.items():
        start_cycle_time = time.time()

        target_name = key
        target_fps = CreateFromBitString(value)  # Converts target fps string in ExplicitBitVect object
        similarity_score_file_name = f"full_similarity_{target_name}"

        file_exists = sim_s3.list_files_in_folder(sc.S3_SIM_FOLDER, f"{similarity_score_file_name}.parquet")
        #   Check if similarity for this target molecule is already exists
        if file_exists:
            logger.info(f'Similarity file for {target_name} exists!')
            logger.info(f'Switching to next target molecule')
            continue
        logger.info(f'New cycle for {target_name} started')

        sim_df = sc.create_full_similarity_df(fps_df, target_name, target_fps)  # Creates full similarity df
        sim_s3.save_parquet_to_s3(sim_df, sc.S3_SIM_FOLDER,
                                  similarity_score_file_name)  # Save full similarity to parquet

        #   Creating top 10 similarities df
        top_10 = sim_df.nlargest(10, 'tanimoto_similarity_score', keep='all')
        top_10.sort_values('tanimoto_similarity_score')

        if len(top_10.index) == 10:
            top_10["has_duplicates_of_last_largest_score"] = False
        elif len(top_10.index) > 10:
            dup_value = top_10.iloc[9]['tanimoto_similarity_score']
            top_10["has_duplicates_of_last_largest_score"] = np.where(top_10['tanimoto_similarity_score'] == dup_value,
                                                                      True, False)
            top_10 = top_10.drop(top_10.index[10:])
        else:
            logger.debug("Less than 10 similarity scores in dataframe")
            top_10["has_duplicates_of_last_largest_score"] = None

        #   Save top 10 similarities in postgres fact table
        save_success = False
        while not save_success:
            save_success = sim_postgres.save_df_to_postgres(top_10)

        end_cycle_time = time.time()

        logger.debug(f"time from start: {end_cycle_time - start}")
        logger.info(f"Similarity computed and saved. Time required: {end_cycle_time - start_cycle_time}")


if __name__ == "__main__":
    compute_similarity()
