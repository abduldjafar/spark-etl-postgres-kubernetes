FROM docker.io/bitnami/spark:3.1.2
ENV SPARK_HOME /opt/bitnami/spark
ADD jars/postgresql-42.2.10.jar $SPARK_HOME/jars/