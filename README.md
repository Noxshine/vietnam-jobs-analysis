# BigData project: Build spark cluster using k8s to intergrate and analysis Vietnam jobs data


- K8s cluster on docker desktop
- Install kubectl
- run deploy-spark-k8s.sh
- run docker-compose
- crawl data put to kafka
- set SPARK_HOME: pyspark
- spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 CareerbuilderMain.py
- 