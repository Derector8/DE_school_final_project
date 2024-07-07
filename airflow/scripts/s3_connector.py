import logging
from io import BytesIO

from airflow.models import Variable
import boto3
import pandas as pd
import pyarrow.parquet as pq


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("S3_Connector_Logger")


class S3Connector:
    """
    Class to represent S3 connection object with credentials.
    Have methods to create boto3 client, check files in folder and save/load csv/parquet files.
    """
    AWS_S3_CREDENTIALS = {
   "key": Variable.get("secret_aws_s3_key_id"),
   "secret": Variable.get("secret_aws_s3_access_key"),
   "token": Variable.get("secret_aws_s3_token"),
   }
    BUCKET_NAME = "de-school-2024-aws"
    INPUT_FOLDER_NAME = "final_task/input_files"

    def __init__(self):

        pass

    @classmethod
    def _s3_client(cls) -> boto3.client:
        """Creating s3 client with boto3"""
        s3_client = boto3.client('s3',
                                 aws_access_key_id=Variable.get("secret_aws_s3_key_id"),
                                 aws_secret_access_key=Variable.get("secret_aws_s3_access_key"),
                                 aws_session_token=Variable.get("secret_aws_s3_token"),
                                 )
        logger.info("S3 client created")
        return s3_client

    @classmethod
    def save_csv_to_s3(cls, df: pd.DataFrame, file_name: str,
                       output_folder: str = "final_task/bukharev_roman/chembl_fps") -> None:
        """Saving pandas df to s3 bucket in csv file"""
        try:
            df.to_csv(
                f"s3://{cls.BUCKET_NAME}/{output_folder}/{file_name}.csv",
                index=False,
                storage_options=cls.AWS_S3_CREDENTIALS,
            )
        except (FileNotFoundError, PermissionError) as e:
            logger.error("Network drive s3 path is not found! Check your connection and permissions.")
            raise e
        except IOError as e:
            logger.error(
                "Network drive s3 path/file is not found! Check carefully the output_folder and file_name provided.")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise e

        logger.info(f"{file_name}.csv  saved!")

    @classmethod
    def save_parquet_to_s3(cls, df: pd.DataFrame, output_folder: str = "final_task/bukharev_roman/chembl_fps",
                           file_name: str = f"chembl_fps") -> None:
        """Saving pandas df to s3 bucket in parquet file"""
        s3_client = cls._s3_client()
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        s3_client.put_object(
            Bucket=cls.BUCKET_NAME,
            Key=f"{output_folder}/{file_name}.parquet",
            Body=out_buffer.getvalue()
        )
        logger.info(f"{file_name}.parquet  saved!")

    @classmethod
    def list_files_in_folder(cls, file_folder: str, file_name_pattern: str = ".csv") -> "list | None":
        """
        Returns list with all bucket files prefixes, which contain provided file name pattern
        or None if no matching files."""
        s3_client = cls._s3_client()
        response = s3_client.list_objects_v2(Bucket=cls.BUCKET_NAME, Prefix=file_folder)

        obj_list = []
        if "Contents" in response:
            for obj in response["Contents"]:
                obj_list.append(obj["Key"])
        else:
            logger.info(f"No files in folder: {cls.BUCKET_NAME}/{file_folder}!")
            return None

        files_list = []
        for key in obj_list:
            if file_name_pattern in key:
                files_list.append(key)
                logger.info(f"{key} object added in list")
            else:
                logger.debug(f"{key} object doesn't match {file_name_pattern}. Skipping it")

        return files_list

    @classmethod
    def load_input_csv_from_s3_to_df(cls, files_list: list) -> "pd.DataFrame | None":
        """
        Loading csv files, included in provided files list and returns df
        or return None in case of errors.
        """
        df_list = []
        for file_name in files_list:
            try:
                df = pd.read_csv(
                    f"s3://{cls.BUCKET_NAME}/{file_name}",
                    storage_options=cls.AWS_S3_CREDENTIALS,
                    index_col=None,
                    header=0,
                    sep=',',
                    encoding="windows-1251",
                    usecols=lambda x: x.lower() in ['molecule name', 'smiles'],
                    on_bad_lines='warn',
                )
            except (FileNotFoundError, pd.errors.EmptyDataError) as e:
                logger.error(f"{e}\nFile not found or empty. Please check carefully file_name")
                return None
            except pd.errors.ParserError as e:
                logger.error(f"{e}\nError with parsing the file")
                return None
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return None
            df.columns = df.columns.str.lower()
            df_list.append(df)

        csv_data_df = pd.concat(df_list, axis=0)

        return csv_data_df

    @classmethod
    def load_parquet_from_s3(cls, files_list: list) -> pd.DataFrame:
        """
        Loading parquet files, included in provided files list and returns df
        """
        s3_client = cls._s3_client()

        df_list = []
        for file in files_list:
            file_obj = s3_client.get_object(Bucket=cls.BUCKET_NAME, Key=file)
            parquet_file = pq.ParquetFile(BytesIO(file_obj["Body"].read()))
            df = parquet_file.read().to_pandas()
            df_list.append(df)
            print(f"One more file loaded - {len(df_list)}")
        return pd.concat(df_list)
