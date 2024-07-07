from airflow.settings import Session
from airflow.utils.db import provide_session
from airflow.models import (
    Connection,
    Variable,
    )
import scripts.credentials as cr
Variable.set(key="secret_aws_postgres_engine", value=cr.AWS_POSTGRES_URI)
Variable.set(key="secret_aws_s3_key_id", value=cr.AWS_S3_CREDENTIALS["key"])
Variable.set(key="secret_aws_s3_access_key", value=cr.AWS_S3_CREDENTIALS["secret"])
Variable.set(key="secret_aws_s3_token", value=cr.AWS_S3_CREDENTIALS["token"])

aws_postgres_conn: Connection = Connection(conn_id="aws_postgres",
                                    conn_type="postgresql",
                                    host=cr.POSTGRES_HOST,
                                    login=cr.POSTGRES_LOGIN,
                                    password=cr.POSTGRES_PASSWORD,
                                    schema=cr.POSTGRES_DATABASE,
                                    port=cr.POSTGRES_PORT,)

session = Session() 
session.add(aws_postgres_conn)
session.commit()