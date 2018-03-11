import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class SendToKafka {
	public static void SendMessage(String topic, String input)  {
		//sudo /usr/bin/kafka_2.11-1.0.0/bin/kafka-server-start.sh /usr/bin/kafka_2.11-1.0.0/config/server.properties
        
        ///usr/bin/kafka_2.11-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic traffic
        ///usr/bin/kafka_2.11-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic alert
        ///usr/bin/kafka_2.11-1.0.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic traffic --from-beginning
        ///usr/bin/kafka_2.11-1.0.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alert --from-beginning
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, null, input);
        
        producer.send(producerRecord);
        producer.close();
    }
}
