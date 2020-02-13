package com.pubsap.eng.streams;

import com.pubsap.eng.schema.Input;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Created by loicmdivad.
 */
public class InputtimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        return ((Input) consumerRecord.value()).getTimestamp().toEpochMilli();
    }
}
