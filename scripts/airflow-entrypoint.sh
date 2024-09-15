#!/usr/bin/env bash
chown -R airflow:airflow /opt/airflow/data/raw
airflow db reset -y
airflow db init
airflow users create -r Admin -u admin -e admin@admin.com -f admin -l admin -p admin

airflow scheduler &
exec airflow webserver