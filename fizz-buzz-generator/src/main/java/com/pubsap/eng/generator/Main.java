package com.pubsap.eng.generator;

import com.pubsap.eng.schema.Input;
import com.pubsap.eng.schema.InputKey;
import com.pubsap.eng.schema.Output;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

/**
 * Created by loicmdivad.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        final Config config = ConfigFactory.load();
        String srConfigKey = "schema.registry.url";

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.ACKS_CONFIG, "0");

        SpecificAvroSerializer<Input> inputSerde = new SpecificAvroSerializer<>();
        SpecificAvroSerializer<InputKey> inputKeySerde = new SpecificAvroSerializer<>();

        inputSerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), false);
        inputKeySerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), true);

        KafkaProducer<InputKey, Input> producer = new KafkaProducer<>(properties, inputKeySerde, inputSerde);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.warn(String.format("Shunting down the generator: %s", Main.class));
            producer.flush();
            producer.close();

        }));

        logger.info(String.format("Starting the generator: %s", Main.class));

        while (true) {
            Thread.sleep(Duration.ofSeconds(1).toMillis());
            Input value = nextInput();
            InputKey key = nextInputKey();
            logger.info("producing: " + value);
            producer.send(new ProducerRecord<>("fizz-buzz-input", key, value));
        }
    }

    public static Input nextInput() {
        return new Input(new Random().nextInt(300), Instant.now());
    }

    public static InputKey nextInputKey() {
        return new InputKey("player-1");
    }
}
