# BigData project: Analizing online Vietnam job using 


helm repo add bitnami-repo https://charts.bitnami.com/bitnami
helm install spark-release bitnami-repo/spark


kubectl port-forward --namespace default svc/spark-release-master-svc 80:80

import to opt/bitnami/spark Careerbuilder/CareerbuiderMain

docker ps




docker cp C:\Users\Admin\Desktop\BigDataProject\spark\Careerbuilder <spark-release-worker-0>:opt/bitnami/spark
kubectl exec -ti --namespace default spark-release-worker-0 -- spark-submit --master spark://spark-release-master-svc:7077 --conf spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 --conf spark.jars.ivy=/tmp/binami/pkg/cache --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 2 Careerbuilder/CareerbuilderMain.py
    
docker cp C:\Users\Admin\Desktop\BigDataProject\spark\Careerlink <spark-release-worker-1>:opt/bitnami/spark
kubectl exec -ti --namespace default spark-release-worker-1 -- spark-submit --master spark://spark-release-master-svc:7077 --conf spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 --conf spark.jars.ivy=/tmp/binami/pkg/cache --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 2 Careerlink/CareerlinkMain.py


docker network connect bigdataproject_spark-network k8s_spark-worker_spark-release-worker-0_default_c5abfb86-4345-4f1a-b5a8-b6a6856159ad_0