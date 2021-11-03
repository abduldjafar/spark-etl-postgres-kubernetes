FROM gcr.io/spark-operator/spark-py:v3.1.1

# add postgres jars
ADD jars/postgresql-42.2.10.jar  $SPARK_HOME/jars
RUN mkdir -p $SPARK_HOME/datas

# add datasets example
ADD data/transaction.csv $SPARK_HOME/datas

# add python script
ADD main.py /opt/spark/examples/src/main/python