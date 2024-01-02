# BigData project: Analizing online Vietnam job
Buid a bigdata storage and processing system for analizing Vietnam job data from recruitment websites Careerlink, Careerbuilder, TopCV,... Data is crawled by [Scrapy](https://scrapy.org/) and processed using [Spark](https://spark.apache.org/). Spark cluster is managed by [Kuberneste](https://kubernetes.io/) inside [Docker](https://www.docker.com/). For testing, we build system in 1 node using Window OS.

## Pipeline


Data crawled from Scrapy is written to a Kafka broker in 2 topic careerlink and careerbuilder. Then spark streaming job subcribe to kafka to read data and make processing. After that, data will be written to mongodb container for further purpose. 
## Installation
- Install all requirements lib.
```bash
pip install -r requirements.txt
```

-  Download Docker desktop [Docker desktop](https://www.docker.com/products/docker-desktop/)
- Install and setup kubectl for Window [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/)
- Install [Helm](https://github.com/helm/helm/releases) - package manager for K8s

## Setup Kuberneste - Spark cluster

### Enable Kuberneste for Docker desktop


- Make sure kubectl point to Kuberneste cluster of Docker desktop
```bash
kubectl get nodes
```

- Create zookeeper, kafka, mongodb containers
```bash
docker-compose up
```
### Create Spark cluster using Helm
- Create a Spark cluster in k8s contain 1 master pod and 2 worker pods

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami

helm install spark-release bitnami/spark
```

- Forward port for Spark web-ui
```bash
kubectl port-forward --namespace default svc/spark-release-master-svc 80:80
```

- Create service for k8s to connect with Kafka and Mongodb container


## Run the system
### Submit job
- Submit careerbuilder job
```bash
docker cp .\BigDataProject\spark\Careerbuilder <your worker0 pod container name>:opt/bitnami/spark

kubectl exec -ti --namespace default spark-release-worker-0 -- spark-submit --master spark://spark-release-master-svc:7077 --conf spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 --conf spark.jars.ivy=/tmp/binami/pkg/cache --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 2 Careerbuilder/CareerbuilderMain.py

```
- Submit careerlink job same as above
- Open spark web ui for application status



### Crawl data
```bash
scrapy crawl careerbuilder

scrapy crawl careerlink
```

### Checking data store in Mongodb
```bash
docker exec -it your_mongo_container_id bin/bash
mongoexport --host localhost --port 27017  --db job-analysis --collection careerbuilder --out careerbuilder.json
mongosh
show dbs
use careerbuilder
db.careerbuilder.find()
```

