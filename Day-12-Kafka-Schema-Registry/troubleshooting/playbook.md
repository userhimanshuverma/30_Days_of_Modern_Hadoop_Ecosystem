# Production Troubleshooting Playbook — Schema Registry & Avro

This runbook contains root causes, diagnostic signals, and remediation strategies for common runtime failures encountered when operating Apache Kafka with Confluent Schema Registry.

---

## 📋 Diagnostics Grid

| Issue | Symptoms | Root Cause | Resolution |
| :--- | :--- | :--- | :--- |
| **Schema Registration Failed (409)** | `SchemaRegistryException: Schema being registered is incompatible with an earlier schema` | Evolved schema violates the subject's compatibility rules (e.g., adding a required field without a default). | 1. Check compatibility level using `curl http://localhost:8081/config/<subject>`. <br> 2. Add a `default` value in the schema for new fields. <br> 3. Temporarily set compatibility level to `NONE` for override, or register under a new subject. |
| **Invalid Schema Payload (422)** | `422 Unprocessable Entity: Invalid Avro schema` | Syntax error in `.avsc` JSON format (missing quotes, trailing commas, or invalid data types). | 1. Validate the Avro schema using an online JSON linter or native compiler. <br> 2. Ensure all types align with Apache Avro specifications. |
| **Schema Not Found (404)** | `40401: Subject not found` or `40403: Schema not found` | The producer registered a schema under `<topic>-value`, but the consumer is searching under a different subject naming strategy. | 1. Inspect subject names in Schema Registry: `curl http://localhost:8081/subjects`. <br> 2. Standardize `key.subject.name.strategy` and `value.subject.name.strategy` properties across clients. |
| **Unknown Schema ID** | `SerializationException: Error deserializing Avro message for id 42: Schema not found` | Consumer read a magic byte and Schema ID, but that ID does not exist in the Schema Registry cache/backend (e.g., consumer points to staging registry, producer points to prod). | 1. Ensure all clients point to the same global Schema Registry cluster. <br> 2. Check the active Schema Registry database by querying: `curl http://localhost:8081/schemas/ids/42`. |
| **Magic Byte Serialization Error** | `SerializationException: Unknown magic byte!` | The consumer was configured with `KafkaAvroDeserializer`, but the incoming Kafka topic message is plain JSON, string, or raw bytes (missing the Confluent 5-byte header). | 1. Set up routing filters or separate topics for legacy vs Avro data. <br> 2. Use a routing consumer that inspects the first byte before selecting the deserializer. |
| **Registry Connection Refused** | `ConnectException: Connection refused` | Schema Registry is offline, behind a failing load balancer, or clients have firewall/network blockages. | 1. Ping the registry port: `telnet <registry-host> 8081`. <br> 2. Check Schema Registry container logs: `docker logs schema-registry-day12`. |
| **Specific Reader Cast Exception** | `ClassCastException: GenericRecord cannot be cast to com.hadoop.schema.User` | The Java consumer is reading events as `User` instances, but `specific.avro.reader` is set to `false`, causing the client to return standard `GenericRecord`. | 1. Set `specific.avro.reader=true` in `consumer.properties`. <br> 2. Ensure generated classes are on the JVM classpath. |
| **Subject Split Brain (Active-Active Sync)** | Schemas registered on Instance A do not replicate to Instance B. | The `_schemas` Kafka topic lacks replication, or the follower Schema Registry is unable to reach the primary coordinator/leader. | 1. Ensure Kafka cluster is healthy. <br> 2. Verify `kafkastore.topic` partitions is set to 1. <br> 3. Verify follower-to-leader network connectivity on the admin port. |

---

## 🛠️ Production Command Toolkit

### 1. View Current Compatibility Mode
Check what rules are enforced on a subject:
```bash
curl -s http://localhost:8081/config/day-12-users-value
```

### 2. Change Compatibility Mode
Change validation rules dynamically to allow structural updates:
```bash
curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "FORWARD"}' \
  http://localhost:8081/config/day-12-users-value
```

### 3. Check compatibility of a Schema draft before deploying
Test if a new local schema file `draft.avsc` is valid:
```bash
ESCAPED_DRAFT=$(cat draft.avsc | jq -Rs .)
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": ${ESCAPED_DRAFT}}" \
  http://localhost:8081/compatibility/subjects/day-12-users-value/versions/latest
```

### 4. Delete a Subject (Soft Delete)
Remove schema history for a subject (soft delete only hides it from lists; add `?permanent=true` for hard delete):
```bash
curl -s -X DELETE http://localhost:8081/subjects/day-12-users-value
```

### 5. Inspect the `_schemas` topic directly from Kafka
To see the raw changelog events stored in the schema database:
```bash
docker exec -it kafka-day12 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic _schemas \
  --from-beginning \
  --property print.key=true
```
