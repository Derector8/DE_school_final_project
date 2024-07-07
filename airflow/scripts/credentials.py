"""
This file contains all credentials, needed for this project.
Please pass all the fields
"""

#   Fill in the fields below with your postgress credentials
POSTGRES_LOGIN = "your_login"

POSTGRES_PASSWORD = "Your_password"

POSTGRES_HOST = "your_postgres_host"

POSTGRES_PORT = 5432   # Default port(change if have conflicts)

POSTGRES_DATABASE = "postgres"

POSTGRES_SCHEMA = "rbukharev"

#   Fill in the fields below with your AWS access credentials
AWS_ACCESS_KEY_ID = "access_key_id"

AWS_SECRET_ACCESS_KEY = "secret_access_key"

AWS_SESSION_TOKEN = "session_token"


#   Fields below will be filled automatically. Please don't touch it!
AWS_S3_CREDENTIALS = {
   "key": AWS_ACCESS_KEY_ID,
   "secret": AWS_SECRET_ACCESS_KEY,
   "token": AWS_SESSION_TOKEN,
   }


AWS_POSTGRES_URI = f"postgresql+psycopg2://{POSTGRES_LOGIN}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
