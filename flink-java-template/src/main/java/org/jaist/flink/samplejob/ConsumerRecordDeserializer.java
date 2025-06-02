package org.jaist.flink.samplejob;

import java.io.IOException;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

/**
 * A custom kafka record deserializer which bundles together the topic, the timestamp
 * and the value (string) as a single, comma-separated string.
 * 
 * @author smarios <smarios@jaist.ac.jp>
 */
public class ConsumerRecordDeserializer implements KafkaRecordDeserializationSchema<String>{

    final private SimpleStringSchema strser = new SimpleStringSchema();
    
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> crs, Collector<String> clctr) throws IOException {
        clctr.collect(String.format("%s,%s,%s", crs.topic(), crs.timestamp(), strser.deserialize(crs.value())));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
