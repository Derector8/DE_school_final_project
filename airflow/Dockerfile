FROM apache/airflow:2.8.1

COPY requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install apache-airflow[amazon,postgres]==${AIRFLOW_VERSION} -r requirements.txt

COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts

