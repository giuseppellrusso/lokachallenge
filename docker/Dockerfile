FROM apache/airflow:2.3.0-python3.9

# install needed linux packages
USER root
RUN set -ex \
    && apt-get update \
    && apt-get install -yq wget unzip iputils-ping \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install

# install needed python modules
USER airflow
COPY entrypoint.sh /opt/airflow/
COPY requirements.txt /opt/airflow/

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
