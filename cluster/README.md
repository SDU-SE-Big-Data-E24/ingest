**Task**: Deploy Kafka using the `helm` and the following [kafka-values.yaml](kafka-values.yaml) file.

```bash
helm install --values kafka-values.yaml kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 30.0.4
```

```bash
kubectl apply -f kafka-schema-registry.yaml
```

```bash
kubectl apply -f kafka-connect.yaml
```

```bash
kubectl apply -f kafka-ksqldb.yaml
```

```bash 
kubectl exec --stdin --tty deployment/kafka-ksqldb-cli -- ksql http://kafka-ksqldb-server:8088
```


```bash
kubectl apply -f redpanda.yaml
```


# HDFS


````bash
kubectl apply -f configmap.yaml
````

````bash
kubectl get configmap hadoop-config
````

````bash
kubectl apply -f namenode.yaml
````


````bash
kubectl apply -f datanodes.yaml
````

```bash
kubectl get pod -w
kubectl describe pod namenode-<ID>
kubectl logs namenode-<ID>
```

# Fetch data
```bash
kubectl apply -f producer.yaml
```
update the fetch-data.yaml file with the correct URL
```bash
kubectl rollout restart deployment producer-data
```
# Port-forwarding
```bash
kubectl port-forward svc/namenode 9870:9870
```

```bash
kubectl port-forward svc/redpanda 8080
```

```bash
curl -s -XGET "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS"
```

```bash
kubectl port-forward svc/kafka-schema-registry 8081
```
```bash
curl http://127.0.0.1:8081
```
```bash
kubectl port-forward svc/kafka-connect 8083
```
```bash
curl http://127.0.0.1:8083
```
```bash
kubectl port-forward svc/kafka 9092
```
```bash
curl http://127.0.0.1:9092
```
```bash
kubectl port-forward service/namenode 9870:9870
```

## Cleanup


```bash
kubectl delete -f sqoop.yaml
helm delete postgresql
kubectl delete pvc data-postgresql-0
kubectl delete pod python
kubectl delete -f flume.yaml
kubectl delete pod kafka-client
kubectl delete -f redpanda.yaml
kubectl delete -f kafka-schema-registry.yaml
kubectl delete -f kafka-connect.yaml
kubectl delete -f kafka-ksqldb.yaml
kubectl delete -f consumer.yaml
kubectl delete -f producer.yaml

kubectl delete -f hdfs-cli.yaml
kubectl delete -f datanodes.yaml
kubectl delete -f namenode.yaml
kubectl delete -f configmap.yaml
```
```bash
helm delete kafka
```
