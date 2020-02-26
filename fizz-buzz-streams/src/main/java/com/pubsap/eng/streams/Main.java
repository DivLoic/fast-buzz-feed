package com.pubsap.eng.streams;

import com.pubsap.eng.schema.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static com.pubsap.eng.common.FizzUtils.mapFromConfig;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * Created by loicmdivad.
 */
public class Main {

    public static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static Topology buildTopology(Config config,
                                         TimeWindows timeWindow,
                                         Serde<Item> itemSerde,
                                         Serde<Input> inputSerde,
                                         Serde<InputKey> inputKeySerde,
                                         Serde<Output> outputSerde,
                                         Serde<OutputKey> outputKeySerde) {

        String inputTopic = config.getString("topic.input.name");
        String outputTopic = config.getString("topic.output.name");

        StreamsBuilder builder = new StreamsBuilder();

        Consumed<InputKey, Input> consumedInputs = Consumed

                .with(inputKeySerde, inputSerde)
                .withTimestampExtractor(new InputTimestampExtractor());

        Produced<OutputKey, Output> producedCounts = Produced.with(outputKeySerde, outputSerde);

        Grouped<InputKey, Item> groupedItem = Grouped.with(inputKeySerde, itemSerde).withName("grouped-item");

        builder.stream(inputTopic, consumedInputs)

                .filterNot(FizzBuzzPredicate.isNoneKey)

                .mapValues(FizzBuzzMapper.parseItem)

                .filterNot(FizzBuzzPredicate.isNoneItem)

                .groupByKey(groupedItem)

                .windowedBy(timeWindow)

                .aggregate(
                        FizzBuzzAggregator.init,
                        FizzBuzzAggregator.aggregator,
                        Named.as("the-grouped-topic"),
                        Materialized.with(inputKeySerde, outputSerde)

                )

                .toStream()

                .map(FizzBuzzMapper.formatOutput)

                .to(outputTopic, producedCounts);

        return builder.build();
    }

    public static void main(String[] args) {
        final Config config = ConfigFactory.load();

        Properties properties = new Properties();

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("application.id"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("reset.offset"));
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        properties.putAll(mapFromConfig(config.getConfig("confluent-cloud-client")));

        Map<String, Object> schemaRegistryConfigMap = mapFromConfig(config.getConfig("schema-registry-client"));
        schemaRegistryConfigMap.put(SCHEMA_REGISTRY_URL_CONFIG, config.getString(SCHEMA_REGISTRY_URL_CONFIG));

        SpecificAvroSerde<Item> itemSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<Input> inputSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<Output> outputSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputKey> outputKeySerde = new SpecificAvroSerde<>();

        itemSerde.configure(schemaRegistryConfigMap, false);
        inputSerde.configure(schemaRegistryConfigMap, false);
        outputSerde.configure(schemaRegistryConfigMap, false);
        inputKeySerde.configure(schemaRegistryConfigMap, true);
        outputKeySerde.configure(schemaRegistryConfigMap, true);

        TimeWindows windows = TimeWindows

                .of(config.getDuration("window.size"))

                .advanceBy(config.getDuration("window.step"));

        Topology topology = buildTopology(
                config,
                windows,
                itemSerde,
                inputSerde,
                inputKeySerde,
                outputSerde,
                outputKeySerde
        );

        logger.info(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.cleanUp();
        streams.start();
    }
}
