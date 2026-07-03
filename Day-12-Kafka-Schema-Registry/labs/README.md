# Hands-on Lab: Java Avro Clients & Maven Code Generation

This directory contains a self-contained Maven project that demonstrates how to implement schema-aware Kafka producers and consumers in Java using the Confluent Schema Registry.

## 🛠️ Project Structure
- `pom.xml`: Maven build file configured with the Confluent Maven Repository, Avro plugins, and compiler targets.
- `src/main/avro/user.avsc`: The Apache Avro schema representing our user account events.
- `src/main/java/.../AvroProducer.java`: The Java class that generates user objects, serializes them via `KafkaAvroSerializer`, and publishes them.
- `src/main/java/.../AvroConsumer.java`: The Java class that consumes events, deserializes them using schema version matching via `KafkaAvroDeserializer`, and commits offsets.

---

## 🚀 How to Run the Java Lab

### Step 1: Start the Kafka Cluster
Ensure the Docker containers are running first:
```bash
docker-compose -f ../docker/docker-compose.yml up -d
```

### Step 2: Compile and Generate Avro Java Classes
Run Maven package to generate sources and build the executable fat JAR.
The `avro-maven-plugin` will read `src/main/avro/user.avsc` and compile it to `com.hadoop.schema.User` under `target/generated-sources/avro/`.

```bash
mvn clean package
```

### Step 3: Run the Java Producer
Use the compiled jar to run the producer, passing the path to the config file:
```bash
java -cp target/day-12-schema-registry-1.0-SNAPSHOT.jar com.hadoop.schema.AvroProducer ../configs/producer.properties
```
*Expected Output:*
You should see 5 logs representing the generation and successful delivery of Avro-serialized messages to Kafka, complete with their partition numbers and offsets.

### Step 4: Run the Java Consumer
To read those records, start the consumer:
```bash
java -cp target/day-12-schema-registry-1.0-SNAPSHOT.jar com.hadoop.schema.AvroConsumer ../configs/consumer.properties
```
*Expected Output:*
The consumer will start up, pull schemas from the local Schema Registry, decode the raw binary stream back into concrete Java instances, and print the fields (`ID`, `Name`, `Email`, `Timestamp`) to the terminal.
