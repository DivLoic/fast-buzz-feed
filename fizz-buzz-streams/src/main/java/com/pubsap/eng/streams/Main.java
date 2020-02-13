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

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.pubsap.eng.schema.ItemValue.*;

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

        String inputTopic = config.getString("input.topic.name");
        String outputTopic = config.getString("output.topic.name");

        StreamsBuilder builder = new StreamsBuilder();

        Consumed<InputKey, Input> inputConsumed =

                Consumed
                        .with(inputKeySerde, inputSerde)
                        .withTimestampExtractor(new InputtimestampExtractor());

        Produced<OutputKey, Output> producedCount = Produced.with(outputKeySerde, outputSerde);

        Grouped<InputKey, Item> groupedItem = Grouped.with(inputKeySerde, itemSerde).withName("grouped-item");

        builder.stream("buzz-feed-input", inputConsumed)

                .filterNot((key, value) -> key.getName().equals("None"))

                .mapValues(value -> {
                    if (value.getValue() % 15 == 0) return new Item(FizzBuzz);
                    else if (value.getValue() % 5 == 0) return new Item(Buzz);
                    else if (value.getValue() % 3 == 0) return new Item(Fizz);
                    else return new Item(None);
                })

                .filterNot((key, value) -> value.getType() == None)

                .groupByKey(groupedItem)

                .windowedBy(timeWindow)

                .aggregate(
                        () -> new Output(0, 0, 0),
                        (key, value, stringLongMap) -> {
                            Output result = null;

                            switch (value.getType()) {
                                case FizzBuzz:
                                    result = new Output(
                                            stringLongMap.getFizz(),
                                            stringLongMap.getBuzz(),
                                            stringLongMap.getFizzBuzz() + 1
                                    );
                                    break;

                                case Buzz:
                                    result = new Output(
                                            stringLongMap.getFizz(),
                                            stringLongMap.getBuzz() + 1,
                                            stringLongMap.getFizzBuzz()
                                    );
                                    break;

                                case Fizz:
                                    result = new Output(
                                            stringLongMap.getFizz(),
                                            stringLongMap.getBuzz(),
                                            stringLongMap.getFizzBuzz()
                                    );
                                    break;
                                default:
                                    result = new Output(0, 0, 0);
                            }
                            return result;
                        },
                        Named.as("the-grouped-topic"),
                        Materialized.with(inputKeySerde, outputSerde)
                ).toStream()

                .map((key, value) -> new KeyValue<>(new OutputKey(
                        key.key().getName(),
                        key.window().startTime().toString(),
                        key.window().endTime().toString()), value)
                )

                .to("buzz-feed-output", Produced.with(outputKeySerde, outputSerde));

        return builder.build();
    }

    public static void main(String[] args) {
        final String srConfigKey = "schema.registry.url";
        final Config config = ConfigFactory.load();

        Properties properties = new Properties();

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("application.id"));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        SpecificAvroSerde<Item> itemSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<Input> inputSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<Output> outputSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<InputKey> inputKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputKey> outputKeySerde = new SpecificAvroSerde<>();

        itemSerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), false);
        inputSerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), false);
        outputSerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), false);
        inputKeySerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), true);
        outputKeySerde.configure(Collections.singletonMap(srConfigKey, config.getString(srConfigKey)), true);

        TimeWindows windows = TimeWindows

                .of(Duration.ofSeconds(20))

                .advanceBy(Duration.ofSeconds(20));

        Topology topology = buildTopology(
                config,
                windows,
                itemSerde,
                inputSerde,
                inputKeySerde,
                outputSerde,
                outputKeySerde
        );

        logger.debug(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.cleanUp();
        streams.start();
    }
}
