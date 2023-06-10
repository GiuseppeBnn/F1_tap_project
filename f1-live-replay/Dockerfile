FROM jupyter/pyspark-notebook:python-3.7.12


WORKDIR /home/jovyan
RUN pip install elasticsearch
ENTRYPOINT [ "spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0","--master","spark://spark-master:7077","--driver-memory","3g","--executor-memory","2g","--executor-cores","1","--num-executors","2", "SparkRegression.py"] 

# "-jars","/home/jovyan/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.0.jar" 
#  "--master","spark://spark-master:7077","--driver-memory","3g","--executor-memory","2g","--executor-cores","1"