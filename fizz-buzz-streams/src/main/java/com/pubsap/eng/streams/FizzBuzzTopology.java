package com.pubsap.eng.streams;

import com.pubsap.eng.schema.*;
import com.typesafe.config.Config;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Created by loicmdivad.
 */
public class FizzBuzzTopology {

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

        Grouped<InputKey, Item> groupedItem = Grouped.with(inputKeySerde, itemSerde).withName("grouped-items");

        Materialized<InputKey, Output, WindowStore<Bytes, byte[]>> materializedItem =
                Materialized.as(String.format("%s-aggregation", config.getString("application.id").toLowerCase()));

        builder.stream(inputTopic, consumedInputs)

                .filterNot(FizzBuzzPredicate.isNoneKey, Named.as("filter-nullkeys-processor"))

                .mapValues(FizzBuzzMapper.parseItem, Named.as("mapper-item-processor"))

                .filterNot(FizzBuzzPredicate.isNoneItem, Named.as("filter-none-processor"))

                .groupByKey(groupedItem)

                .windowedBy(timeWindow)

                .aggregate(

                        FizzBuzzAggregator.init,

                        FizzBuzzAggregator.aggregator,

                        Named.as("aggregate-processor"),

                        materializedItem.withKeySerde(inputKeySerde).withValueSerde(outputSerde)
                )

                .toStream(Named.as("tostream-processor"))

                .map(FizzBuzzMapper.formatOutput, Named.as("mapper-keyformat-processor"))

                .to(outputTopic, producedCounts);

        return builder.build();
    }
}
