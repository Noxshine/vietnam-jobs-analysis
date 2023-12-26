# BigData project: Build spark cluster using k8s to intergrate and analysis Vietnam jobs data


- K8s cluster on docker desktop
- Install kubectl
- run deploy-spark-k8s.sh
- run docker-compose
- crawl data put to kafka
- spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 CareerbuilderMain.py
- kubectl get nodes


# access kafka sh
	- docker exec -it  kafka /bin/sh

# check data topic
	- kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic careerbuilder --from-beginning

kubectl get pods

#OPEN WEB UI SPARK
	kubectl port-forward service/spark-master 7777:7777

