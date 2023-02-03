#!/usr/bin/env bash

# Install custom python package if requirements.txt is present
if [ -e "/opt/airflow/requirements.txt"  ]; then
    $(command -v pip) install --user -r /opt/airflow/requirements.txt
fi

case "$1" in
  webserver)
    airflow db init
    airflow users create \
    --username airflow \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org \
    --password airflow

    num_tries=60
    while [ ${num_tries} != 0 ]; do
      airflow_check=$(airflow db check)
      if [[ $airflow_check == *"Connection successful"* ]]  ; then

        airflow connections add --conn-type 'postgres' --conn-host 'postgres' --conn-login ${POSTGRES_USER} --conn-password ${POSTGRES_PASSWORD} --conn-port 5432 --conn-schema 'postgres' 'loka_dwh'
        airflow connections add --conn-type 'aws' --conn-extra '{"host": "'"http://localstack:4566"'"}' 'loka_aws'

        num_tries=0
      else
        num_tries=$((num_tries - 1))
        sleep 3
      fi
    done

    airflow scheduler &
    exec airflow webserver --port 8080
    ;;
  worker|scheduler)
    # To give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
