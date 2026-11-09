FROM apache/airflow:3.4.0b2-python3.14

USER airflow

COPY airflow_image/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt