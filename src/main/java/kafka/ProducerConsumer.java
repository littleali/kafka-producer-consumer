package kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class ProducerConsumer {

    static Map<String, Long> summation = new HashMap<String, Long>();
    static Map<String, Long> squaredSummation = new HashMap<String, Long>();
    static Map<String, Double> variance = new HashMap<String, Double>();
    static Map<String, Double> mean = new HashMap<String, Double>();
    static Map<String, Long> N = new HashMap<String, Long>();
    

    public static void main(String[] args) {
        //run the program with args: producer/consumer broker:port
        String groupId = "my-group", brokers = "", topic = "logs-output", type = "";
        if (args.length == 2) {
            type = args[0];
            brokers = args[1];
        } else {
            type = "producer";
            brokers = "localhost:9092";
        }

        Properties props = new Properties();

        //configure the following three settings for SSL Encryption
        // props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        // props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/pki/tls/keystore.jks");
        // props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password");
        // props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "password");

        if (type.equals("consumer")) {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            // props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            TimeWindowedSerde<String> windowedSerde = new WindowedSerdes.TimeWindowedSerde<String>(Serdes.String());

            
            KafkaConsumer < Windowed<String> ,Long > consumer = 
            new KafkaConsumer<Windowed<String> ,Long>
            (props, windowedSerde.deserializer(), Serdes.Long().deserializer());
            TestConsumerRebalanceListener rebalanceListener = new TestConsumerRebalanceListener();
            consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
            final int giveUp = 10000000;
            int noRecordsCount = 0;

            
            while (true) {
                
                final ConsumerRecords < Windowed<String>, Long > consumerRecords =
                    consumer.poll(Duration.ofSeconds(1));

                if (consumerRecords.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > giveUp) break;
                    else continue;
                }

                consumerRecords.forEach(record -> {
                    System.out.printf("Consumer Record:(%s , %s , %s, %s, %d, %d)\n"
                        , record.key().key()
                        , new Date(record.key().window().start()).toString()
                        , new Date(record.key().window().end()).toString()
                        , record.value(),
                        detectAnomaly(record.key().key(), record.value());
                        
                        record.partition(), record.offset());
                });

                consumer.commitAsync();
            }
            consumer.close();
            System.out.println("DONE");

        } else {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer < String, String > producer = new KafkaProducer < > (props);
            TestCallback callback = new TestCallback();
            Random rnd = new Random();
            // So we can generate random sentences
            Random random = new Random();
            String[] sentences = new String[] {
                "beauty is in the eyes of the beer holders",
                "Apple is not a fruit",
                "Cucumber is not vegetable",
                "Potato is stem vegetable, not root",
                "Python is a language, not a reptile"
            };
            for (int i = 0; i < 1000; i++) {
                // Pick a sentence at random
                String sentence = sentences[random.nextInt(sentences.length)];
                // Send the sentence to the test topic
                ProducerRecord < String, String > data = new ProducerRecord < String, String > (
                    topic, sentence);
                long startTime = System.currentTimeMillis();
                producer.send(data, callback);
                long elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("Sent this sentence: " + sentence + " in " + elapsedTime + " ms");

            }
            System.out.println("Done");
            producer.flush();
            producer.close();
        }

    }

    private static boolean detectAnomaly(String key, Long value) {
        Double varianceAlertThresholdRatio = 0.12;
        Double maxMeanThreshold = 1000.0;
        Double meanComparisonThresholdRatio = 0.10;
        if(!summation.containsKey(key)){
            summation.put(key, 0L);
            squaredSummation.put(key, 0L);
            variance.put(key, 0.0);
            mean.put(key, 0.0);
            N.put(key, 1L);
        }
        //Calculate sum x , sum x^2
        Long oldSummation = summation.get(key);
        Long currentNumber = value - oldSummation;
        summation.put(key, value);
        squaredSummation.put(key, squaredSummation.get(key) + currentNumber* currentNumber);

        Double oldVariance = variance.get(key);
        Double oldMean = mean.get(key);

        Long index = N.get(key);
        Double newVariance = (1.0/index) * (squaredSummation.get(key) 
        - (Math.pow(summation.get(key), 2)/index) );
        Double newMean = (oldMean * (index - 1) + currentNumber) / (index);

        variance.put(key, newVariance);
        mean.put(key, newMean);
        N.put(key, index + 1);
        
        //Check History
        if(Math.abs(newVariance - oldVariance) > varianceAlertThresholdRatio * oldVariance){
            System.out.println("Anomaly detected in variance.");
        }

        //Check Boundry
        if(newMean > maxMeanThreshold){
            System.out.println("Mean passed the maximum threshold.");
        }

        //Check Log Balancing in each server
        for(String otherKey: mean.keySet()){
            if(otherKey.equals(key)){
                continue;
            }
            if(Math.abs(mean.get(key) - mean.get(otherKey)) > 
                 mean.get(otherKey) * meanComparisonThresholdRatio){
                     System.out.println("Mean divergency detected.");
            }
            
        }

        return true;

    }

    private static class TestConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection < TopicPartition > partitions) {
            System.out.println("Called onPartitionsRevoked with partitions:" + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection < TopicPartition > partitions) {
            System.out.println("Called onPartitionsAssigned with partitions:" + partitions);
        }
    }

    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }

}
