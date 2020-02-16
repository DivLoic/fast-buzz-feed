package com.pubsap.eng.streams;

import com.pubsap.eng.schema.Input;
import com.pubsap.eng.schema.InputKey;
import com.pubsap.eng.schema.Item;
import org.apache.kafka.streams.kstream.Predicate;

import static com.pubsap.eng.schema.ItemValue.None;

/**
 * Created by loicmdivad.
 */
public class FizzBuzzPredicate {

    public static final Predicate<InputKey, Input> isNoneKey = (key, value) -> key.getName().equals("None");

    public static final Predicate<InputKey, Item> isNoneItem = (key, value) -> value.getType() == None;
}
