FROM spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
RUN pip install --no-cache-dir numpy==1.26.4 pandas==2.2.3 pyarrow==17.0.0 \
    pyspark==3.5.3 "dbt-core>=1.8,<1.10" "dbt-spark[session]>=1.8,<1.10"
