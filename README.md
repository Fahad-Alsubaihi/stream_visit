# stream_visit

## Steps:

### 1- Create 2 Topics <product_visit,product_purchased> with the schema below and <product_visit_temp,potential_buyer>

Schema:
Topic <product_visit,product_purchased> 
```
{
  "fields": [
    {
      "name": "customerMobile",
      "type": "string"
    },
    {
      "name": "productId",
      "type": "string"
    },
    {
      "name": "timeOfPurchase",
      "type": "string"
    }
  ],
  "name": "myrecord",
  "type": "record"
}
```
---
### 2- Run the Kafka CLI follow the link [Run the KAFKA CLI](https://github.com/Fahad-Alsubaihi/Kafka-Confluent-SingleStore-Docker-compose/blob/main/Install-Confluent-and-SingleStore.md#:~:text=If%20you%20want%20to%20run%20confluent%20CLI%20follow%20these%3A)

## Note : Change the command based on your cluster and run 2 CLI one for each Topic

producers:

1- Topic <product_visit>:
```

kafka-avro-console-producer --topic product_visit --bootstrap-server broker:9093 --property schema.registry.url=http://schema-registry:8081 --property value.schema.id=6 --property parse.key=true --property key.separator=":" --property key.schema='{"type":"string"}' --property key.serializer=org.apache.kafka.common.serialization.StringSerializer
```
2- Topic <product_purchased>:
```
kafka-avro-console-producer --topic product_purchased --bootstrap-server broker:9093 --property schema.registry.url=http://schema-registry:8081 --property value.schema.id=6 --property parse.key=true --property key.separator=":" --property key.schema='{"type":"string"}' --property key.serializer=org.apache.kafka.common.serialization.StringSerializer
```


consumer:

Topic <potential_buyer>:

```
kafka-avro-console-consumer --topic potential_buyer --bootstrap-server broker:9093 --property schema.registry.url=http://schema-registry:8081 --property print.key=true --from-beginning --key-deserializer org.apache.kafka.common.serialization.StringDeserializer
```

masseges:

Topic <product_visit>

now --------->  
```
"1":{"customerMobile":"12","productId":"d2","timeOfPurchase":""}
```
after 20sec -> 

```
"2":{"customerMobile":"12","productId":"d2","timeOfPurchase":""}
```
after 35sec -> 
```
"1":{"customerMobile":"12","productId":"d2","timeOfPurchase":""}
```
---

Topic <product_purchased>

now --------->  
```
"1":{"customerMobile":"12","productId":"d2","timeOfPurchase":""}
```
