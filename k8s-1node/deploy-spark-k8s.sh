#!/bin/bash

# Deploy Spark ConfigMap
kubectl apply -f spark-configmap.yaml

# Deploy Spark Master Service
kubectl apply -f spark-service.yaml

# Deploy Spark Master Deployment
kubectl apply -f spark-master.yaml

# Deploy Spark Worker Deployment
kubectl apply -f spark-worker.yaml
