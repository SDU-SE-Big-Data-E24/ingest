**Task**: Deploy Kafka using the `helm` and the following [kafka-values.yaml](kafka-values.yaml) file.

```bash
helm install --values kafka-values.yaml kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 30.0.4
```
# Kafka
```bash
kubectl apply -f kafka/.
```
```bash
kubectl get pod -w
```
```bash
kubectl port-forward svc/redpanda 8080

```
```bash
kubectl port-forward svc/kafka-schema-registry 8081

```
```bash
kubectl port-forward svc/kafka-connect 8083
```

# HDFS & storage
```bash
kubectl apply -f storage/.
```
```bash
kubectl get pod -w
```
```bash
kubectl port-forward service/redis 6379:6379
```
```bash
kubectl port-forward svc/namenode 9870:9870
```
```bash
kubectl port-forward svc/namenode 9000:9000
```
# Fetch data
```bash
kubectl apply -f producers/producer-energinet-consumption-industry.yaml
```
```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"ConsumptionIndustry\\\",\\\"namespace\\\":\\\"big_data\\\",\\\"fields\\\":[{\\\"name\\\":\\\"HourUTC\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"HourDK\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"MunicipalityNo\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"Branche\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"ConsumptionkWh\\\",\\\"type\\\":\\\"float\\\"}]}\"}" http://localhost:8081/subjects/ConsumptionIndustry-value/versions
```
```bash
curl -X GET http://localhost:8081/subjects/ConsumptionIndustry-value/versions/latest
```

```bash
curl -X POST -H "Content-Type: application/json" -d @configuration.json http://127.0.0.1:8083/connectors

```
```bash
curl -X POST -H "Content-Type: application/json" -d "{ 
    \"name\": \"hdfs-sink\", 
    \"config\": {
        \"connector.class\": \"io.confluent.connect.hdfs.HdfsSinkConnector\",
        \"tasks.max\": \"3\",
        \"topics\": \"ConsumptionIndustry\",
        \"hdfs.url\": \"hdfs://namenode:9000\",
        \"flush.size\": \"3\",
        \"format.class\": \"io.confluent.connect.hdfs.json.JsonFormat\",
        \"key.converter.schemas.enable\": \"false\",
        \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
        \"key.converter.schema.registry.url\": \"http://kafka-schema-registry:8081\",
        \"value.converter.schemas.enable\": \"false\",
        \"value.converter.schema.registry.url\": \"http://kafka-schema-registry:8081\",
        \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\"
    }
}" http://127.0.0.1:8083/connectors
```

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092  --describe --group _confluent-ksql-kafka-ksqldb-group-id-01transient_transient_STREAM_CONSUMPTION_INDUSTRY_6448920560480449782_1734424504838


```

```bash
curl -X PUT -H "Content-Type: application/json" -d @configuration.json http://127.0.0.1:8083/connectors/kafka-connect-group-id-01/config
```
```bash
curl -X PUT -H "Content-Type: application/json" -d "{     
    \"connector.class\": \"io.confluent.connect.hdfs.HdfsSinkConnector\",
    \"tasks.max\": \"3\",
    \"topics\": \"ConsumptionIndustry\",
    \"hdfs.url\": \"hdfs://namenode:9000\",
    \"flush.size\": \"3\",
    \"format.class\": \"io.confluent.connect.hdfs.json.JsonFormat\",
    \"key.converter.schemas.enable\": \"false\",
    \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
    \"key.converter.schema.registry.url\": \"http://kafka-schema-registry:8081\",
    \"value.converter.schemas.enable\": \"true\",
    \"value.converter.schema.registry.url\": \"http://kafka-schema-registry:8081\",
    \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\"
}" http://127.0.0.1:8083/connectors/hdfs-sink/config
```



```bash
kubectl exec --stdin --tty deployment/kafka-ksqldb-cli -- ksql http://kafka-ksqldb-server:8088
```

```sql
CREATE STREAM STREAM_INGESTION (
    HourUTC STRING,
    HourDK STRING,
    MunicipalityNo STRING,
    Branche STRING,
    ConsumptionkWh DOUBLE
) WITH (
    KAFKA_TOPIC = 'ConsumptionIndustry',
    VALUE_FORMAT = 'AVRO'
);
```


```bash
curl -s -XGET "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS"
```

```bash
curl http://127.0.0.1:8081
```
```bash
curl http://127.0.0.1:8083
```
```bash
curl http://127.0.0.1:9092
```

update the fetch-data.yaml file with the correct URL
```bash
kubectl rollout restart producer-energinet-consumption-industry
```







## Cleanup
```bash
kubectl get pod -w
kubectl delete all --all
kubectl delete pvc --all
helm delete kafka
```

