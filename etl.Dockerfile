FROM docker.io/bitnami/spark:3.1.2
USER root
ENV SPARK_HOME /opt/bitnami/spark
ADD main.py .
ADD etl_process.sh .
ADD requirement.txt .
RUN pip install -r requirement.txt
CMD [ "bash","etl_process.sh" ]