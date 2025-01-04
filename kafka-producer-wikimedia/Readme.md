## Running the Wikimedia Changes Producer

This module demonstrates how to stream real-time events from the Wikimedia recent changes feed and publish them to a Kafka topic.

### Steps to Set Up

1. **Create the Kafka Topic**:  
   Run the following command to create the `wikimedia.recentchange` topic with 3 partitions:

   ```bash
   kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wikimedia.recentchange --partitions 3
   ```

2. **Start the Producer**:  
   Run the `WikiMediaChangesProducer` class to start streaming events from Wikimedia and publishing them to the Kafka topic.

3. **Start a Consumer**:  
   To listen to the events being published, run the following command:

   ```bash
   kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wikimedia.recentchange
   ```

   This will display the real-time events from the `wikimedia.recentchange` topic in your console.

### How It Works

The producer connects to the [Wikimedia EventStreams API](https://stream.wikimedia.org/v2/stream/recentchange) and listens for recent changes. Each event is logged and sent to the Kafka topic `wikimedia.recentchange` using a Kafka producer.

### Example Output

When the producer is running, the consumer will display events like this:

```json
{
   "type": "edit",
   "title": "Apache Kafka",
   "user": "exampleUser",
   "timestamp": "1672931282",
   "comment": "Updated the description of Kafka."
}
```
