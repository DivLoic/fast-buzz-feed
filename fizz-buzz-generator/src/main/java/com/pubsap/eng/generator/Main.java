package com.pubsap.eng.generator;

import com.pubsap.eng.schema.Input;
import com.pubsap.eng.schema.InputKey;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static com.pubsap.eng.generator.FizzUtils.mapFromConfig;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * Created by loicmdivad.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        final Config config = ConfigFactory.load();
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.ACKS_CONFIG, "0");

        properties.putAll(mapFromConfig(config.getConfig("confluent-cloud-client")));

        SpecificAvroSerializer<Input> inputSerde = new SpecificAvroSerializer<>();
        SpecificAvroSerializer<InputKey> inputKeySerde = new SpecificAvroSerializer<>();

        Map<String, Object> schemaRegistryConfigMap = mapFromConfig(config.getConfig("schema-registry-client"));

        schemaRegistryConfigMap.put(SCHEMA_REGISTRY_URL_CONFIG, config.getString(SCHEMA_REGISTRY_URL_CONFIG));

        inputSerde.configure(schemaRegistryConfigMap, false);
        inputKeySerde.configure(schemaRegistryConfigMap, true);

        KafkaProducer<InputKey, Input> producer = new KafkaProducer<>(properties, inputKeySerde, inputSerde);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.warn(String.format("Shunting down the generator: %s", Main.class));
            producer.flush();
            producer.close();

        }));

        logger.info(String.format("Starting the generator: %s", Main.class));

        Runtime
                .getRuntime()
                .addShutdownHook(new Thread(() -> {
                    logger.error("Fizz Buzz Generator has been interrupted.");
                    logger.error("Closing Generator now!");
                }));

        while (true) {

            logger.debug("Wait for generation");
            Thread.sleep(nextDelay(config).toMillis());

            logger.debug("Generating Key");
            Input value = nextInput(config);

            logger.debug("Generating Value");
            InputKey key = nextInputKey(config);

            logger.info("Producing input value: " + value);
            producer.send(
                    new ProducerRecord<>(config.getString("topic.input.name"), key, value),
                    (recordMetadata, exception) -> {

                        if (Optional.ofNullable(exception).isPresent())
                            logger.error("Fail to produce element ", exception);
                        else {
                            logger.debug("Successfully send an generated event");
                            logger.debug("topic = " + recordMetadata.topic() +
                                    ", partition = " + recordMetadata.partition() +
                                    ", offset = " + recordMetadata.offset() +
                                    ", timestamp = " + recordMetadata.timestamp() +
                                    ",  serializedKeySize = " + recordMetadata.serializedKeySize() +
                                    ", serializedValueSize = %s ", recordMetadata.serializedValueSize());
                        }
                    }
            );

        }
    }

    public static Duration nextDelay(Config config) {
        int maxValue = config.getInt("max.generated.delay.ms");
        return Duration.ofMillis(new Random().nextInt(maxValue));
    }

    public static Input nextInput(Config config) {
        int maxValue = config.getInt("max.generated.value");
        return new Input(new Random().nextInt(maxValue), Instant.now());
    }

    public static InputKey nextInputKey(Config config) {
        return new InputKey(config.getString("client.id"));
    }
}
