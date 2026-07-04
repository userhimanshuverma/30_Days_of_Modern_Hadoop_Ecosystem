# Production Troubleshooting Guides

This folder is a fast reference for debug commands and operational diagnostics.

## Quick CLI Commands

### Describe consumer group offsets:
```bash
docker exec -it kafka-day13 kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group clickstream-storage-writer-group
```

### Reset consumer offsets to earliest (for event replay):
```bash
docker exec -it kafka-day13 kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group clickstream-storage-writer-group \
    --reset-offsets --to-earliest --execute --topic clickstream-events
```

### Consume raw messages from topic console for debugging:
```bash
docker exec -it kafka-day13 kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic clickstream-events --from-beginning --max-messages 10
```

Refer to the main [README.md](../README.md#section-12--production-troubleshooting-playbook) for full symptom tables and resolutions.
