package com.pubsap.eng.streams;

import com.pubsap.eng.schema.InputKey;
import com.pubsap.eng.schema.Item;
import com.pubsap.eng.schema.Output;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;

/**
 * Created by loicmdivad.
 */
public class FizzBuzzAggregator {

    public static final Initializer<Output> init = () -> new Output(0, 0, 0);

    public static final Aggregator<InputKey, Item, Output> aggregator = (key, value, stringLongMap) -> {
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
                        stringLongMap.getFizz() + 1,
                        stringLongMap.getBuzz(),
                        stringLongMap.getFizzBuzz()
                );
                break;
            default:
                result = new Output(0, 0, 0);
        }
        return result;
    };
}
