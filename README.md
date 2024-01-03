# BigData project: Analizing online Vietnam job
Buid a bigdata storage and processing system for analizing Vietnam job data from recruitment websites Careerlink, Careerbuilder, TopCV,... Data is crawled by [Scrapy](https://scrapy.org/) and processed using [Spark streaming](https://spark.apache.org/). Spark cluster created inside [Docker](https://www.docker.com/).


## Pipeline

Data crawled from 2 websites [Careerbuilder](https://careerbuilder.vn/) and [Careerlink](https://www.careerlink.vn/) by Scrapy is written to a Kafka cluster. Then spark streaming job subcribe to these topic to read data and make processing. After that, data will be written to [Mongodb](https://www.mongodb.com/) container for further purposes. Kafka, Spark, Mongodb are created in containers running on Docker

![Alt text](pipeline.png)

## Installation

- Install all requirement libraries
```bash
pip install -r requirements.txt
```

- Download Docker desktop [Docker desktop](https://www.docker.com/products/docker-desktop/)

- Build spark cluster, kafka and mongodb containers
```bash
docker-compose up
```
- Check spark cluster web ui on "localhost:8080"

*******images*********

- Create kafka topic

For example: create topic careerbuilder in kafka-1 broker

```bash
docker exec -it  kafka-1 /bin/sh kafka-1.sh --bootstrap-server localhost:9092 --topic careerbuilder --create --partitions 3 --replication-factor 1

```
## Running

- Copy py. files of each job to a spark workder dir
```bash
docker cp \BigDataProject\spark\Careerbuilder    bigdataproject-spark-worker-1-1:opt/bitnami/spark

docker cp \BigDataProject\spark\Careerlink    bigdataproject-spark-worker-2-1:opt/bitnami/spark
```

- Submmit job to spark cluster

```bash
docker exec -it bigdataproject-spark-worker-1-1 /bin/bash spark-submit --master spark://spark-master:7077 --conf spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 --conf spark.jars.ivy=/tmp/binami/pkg/cache --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 2 Careerbuilder/CareerbuilderMain.py

```
In spark web UI there are job submitted is running


- Run scrapy to crawl data

```bash
scrapy crawl careerbuilder
```
```bash
scrapy crawl careerlink
```

- Check Mongodb container for output

```bash
docker exec -it bigdataproject-mymongodb
mongosh
show dbs
use job-analysis
db.careerbuilder.find()
```