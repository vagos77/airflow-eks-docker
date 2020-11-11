FROM apache/airflow:1.10.12-python3.7

LABEL version="1.0.3"

RUN pip install --user pytest

RUN pip install --no-cache-dir --user --upgrade requests==2.23.0 && pip install --no-cache-dir --user snowflake-sqlalchemy snowflake-connector-python vim

RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "vim"]

COPY dags/ ${AIRFLOW_HOME}/dags
COPY scripts/ ${AIRFLOW_HOME}/scripts
COPY unittests.cfg ${AIRFLOW_HOME}/unittests.cfg
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY unittests/ ${AIRFLOW_HOME}/unittests
COPY integrationtests ${AIRFLOW_HOME}/integrationtests