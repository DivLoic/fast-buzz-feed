package com.pubsap.eng.streams;

import com.pubsap.eng.schema.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import static com.pubsap.eng.schema.ItemValue.*;

/**
 * Created by loicmdivad.
 */
public class FizzBuzzMapper {

    public static final ValueMapper<Input, Item> parseItem = value -> {
        if (value.getValue() % 15 == 0) return new Item(FizzBuzz);
        else if (value.getValue() % 5 == 0) return new Item(Buzz);
        else if (value.getValue() % 3 == 0) return new Item(Fizz);
        else return new Item(None);
    };

    public static final KeyValueMapper<Windowed<InputKey>, Output, KeyValue<OutputKey, Output>> formatOutput =
            (key, value) -> new KeyValue<>(new OutputKey(
                    key.key().getName(),
                    key.window().startTime().toString(),
                    key.window().endTime().toString()), value
            );
}
