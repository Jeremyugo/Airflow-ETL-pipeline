FROM apache/airflow:2.10.1-python3.9

USER root

ARG AIRFLOW_HOME=/opt/airflow
ADD data/raw /opt/airflow/data/raw
ADD dags /opt/airflow/dags
ADD scripts /opt/airflow/scripts

RUN mkdir -p /opt/airflow/data/raw \
    && chown -R airflow: ${AIRFLOW_HOME}

USER airflow
 
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /requirements.txt

USER ${AIRFLOW_UID}