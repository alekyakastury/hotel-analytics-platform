FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create paths your code expects
RUN mkdir -p /opt/data/tmp \
 && chown -R airflow:0 /opt/data \
 && chmod -R 775 /opt/data

USER airflow

WORKDIR /opt/project
COPY . /opt/project

# Expose /dbt -> /opt/project/dbt
USER root
RUN ln -s /opt/project/dbt /dbt \
 && chown -h airflow:0 /dbt
USER airflow

RUN if [ -f /opt/project/requirements.txt ]; then \
      pip install --no-cache-dir -r /opt/project/requirements.txt; \
    fi

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV PYTHONPATH=/opt/project
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags

COPY --chown=airflow:airflow entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
