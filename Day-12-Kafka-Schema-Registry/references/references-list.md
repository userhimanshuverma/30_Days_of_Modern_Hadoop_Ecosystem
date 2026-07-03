# References & Deep Reads — Day 12

Below is a curated compilation of documentation, whitepapers, and engineering blog posts covering Apache Avro, Schema Registry, and schema evolution in large-scale event streaming pipelines.

## 📖 Official Documentation
- **Confluent Schema Registry Docs**: [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- **Apache Avro Specification**: [Apache Avro 1.11.3 Spec](https://avro.apache.org/docs/1.11.3/specification/)
- **Kafka Clients Serialization Guides**: [Kafka Serializers and Deserializers](https://docs.confluent.io/platform/current/clients/index.html)

## 🔬 Whitepapers & Architecture Specifications
- **Avro Schema Resolution Rules**: Details on how Avro reader and writer schemas are matched. [Avro Schema Resolution](https://avro.apache.org/docs/1.11.3/specification/#schema-resolution)
- **The Confluent Wire Format**: Overview of the 5-byte header prepended to serialized records. [Schema Registry Wire Format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop-avro.html#wire-format)

## 🏢 Enterprise Engineering Blogs
- **LinkedIn Engineering**: *Schema Evolution in Avro and Protocol Buffers* (LinkedIn was the pioneer of Kafka and early Schema Registry concepts).
- **Confluent Blog**: *Schema Registry: Decoupling Producers and Consumers* by Gwen Shapira.
- **Uber Engineering**: *Handling Schema Evolution at Scale in Uber's Data Pipelines*.
- **Netflix Tech Blog**: *How Netflix manages schemas in its Real-Time Event Ingestion Platform*.
