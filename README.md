# confluent-keda-poc
### Description
This repo is to:
- Demo Keda connection to confluent
- Demo scaling capabilities of Keda

## Set up
### Kubernetes
Used Minikube
To get endpoint for minikube 
```
minikube service dep --url
```

### Go
```
touch main.go
// Add in basic server codes
go mod init confluent-keda-poc
go mod tidy
```

### Keda
Reference: 
- https://keda.sh/docs/2.7/deploy/
- https://keda.sh/docs/2.8/scalers/apache-kafka/#example
```
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
kubectl create namespace keda
helm install keda kedacore/keda --namespace keda
```

## Observations
### Kafka
1. Spammed produce endpoint 
```
http://127.0.0.1:56825/api/produce
```

2. Confluent Cloud Consumer Lag
- Keda Consumer Lag Setting: `lagThreshold: "50"`
    ![image.png](images/confluent-cloud-consumer-lag.png)

3. Scaling up

    ![image.png](images/kube-events.png)
    ![image.png](images/scaling-up.png)

4. Scaling down

    ![image.png](images/scaling-down.png)

5. Scaling down to below original replicas of 3
- Keda Consumer Config: `minReplicaCount:  1`

    ![image.png](images/scaled-below-original.png)

### Kafka Total Lag
- specified to a scaling target of 100 total consumer lag

1. Total Kafka consumer lag
- Note that Specific topic scaling is not set for `topic_2`. This demo shows that KEDA is triggering based on total Kafka Consumer Lag

    ![image.png](images/kafka-total-lag.png)

2. HPA Trigger Scale Up to 2 replicas
- The name of the external metrics also points to the trigger for total Kafka Consumer Lag

    ![image.png](images/hpa-trigger-scale-up.png)

3. 2 Replicas

    ![image.png](images/scaling-up-kafka-total-lag.png)


### CPU
1. Hit 20% Average CPU, Scaling up

    ![image.png](images/cpu-scale-up.png)

- cpu scaler need HPAContainerMetrics feature enabled

### Cron
1. Before Cron scaling

    ![image.png](images/before-cron-scale.png)

2. After Cron scaling

    ![image.png](images/after-cron-scale.png)

3. CPU Scaling during Cron scaling period

    ![image.png](images/cpu-scaling-during-cron.png)


## Issues to think about
### Kafka-scaler
- Message stuck due to error
- Partition Key specified for topic. Large consumer lag observed on one/few particular partitions.
- Metric watched is the max consumer lab for single partition

### CPU-Scaler
- `containerName` parameter requires Kubernetes cluster version `1.20 or higher` with `HPAContainerMetrics` feature enabled.


## Others
To Observe and kill process on local
```
netstat -anop | grep -i 5000
pkill <PID>
kill -9 <PID>
```


