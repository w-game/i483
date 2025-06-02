/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jaist.flink.samplejob;

import java.time.Duration;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.cep.PatternStream;
import java.util.List;
/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

        @SuppressWarnings("ResultOfObjectAllocationIgnored")
	public static void main(String[] args) throws Exception {
            System.out.println("hello, world, from flink!");
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                env.getConfig().setAutoWatermarkInterval(1000);

		buildPipeline(env);
	}

    public static void buildPipeline(StreamExecutionEnvironment env) throws Exception {
        KafkaSource<String> ks = KafkaSource.<String>builder()
            .setBootstrapServers("150.65.230.59:9092")
            .setTopicPattern(java.util.regex.Pattern.compile("i483-allsensors"))
            // .setTopicPattern(java.util.regex.Pattern.compile("i483-sensors-s2410083-(?!analytics-).*"))
            // .setTopics("i483-allsensors")
            .setDeserializer(new ConsumerRecordDeserializer())
            .setStartingOffsets(OffsetsInitializer.latest())
            .setProperty("properties.metadata.max.age.ms", "5000")
            .build();

        SingleOutputStreamOperator<String> analyticsStream = env.fromSource(
                ks,
                WatermarkStrategy.<String>forMonotonousTimestamps()
                    .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                        @Override
                        public long extractTimestamp(String element, long recordTimestamp) {
                            String[] parts = element.split(",", 3);
                            return Long.parseLong(parts[1]);
                        }
                    })
                    .withIdleness(Duration.ofSeconds(5)),
                "Kafka Source")
                .map(new LineToTupleMapper())
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.DOUBLE))
                .keyBy(new SensorKeySelector())
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
                .process(new ProcessWindowFunction<
                        Tuple5<String, String, String, Long, Double>,
                        String,
                        String,
                        TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<Tuple5<String, String, String, Long, Double>> elements,
                                        Collector<String> out) {
                        int count = 0;
                        double sum = 0.0, min = Double.MAX_VALUE, max = Double.MIN_VALUE;
                        for (Tuple5<String, String, String, Long, Double> e : elements) {
                            count++;
                            double v = e.f4;
                            sum += v;
                            min = Math.min(min, v);
                            max = Math.max(max, v);

                            System.out.println("[DEBUG] Processing element: " + e);
                        }
                        System.out.println("Window Triggered: key=" + key + ", count=" + count);
                        double avg = sum / count;
                        long windowEnd = ctx.window().getEnd();
                        String sid = "s2410083";
                        String[] parts = key.split("-");
                        String sensor = parts[0], dataType = parts[1];

                        out.collect(String.format("i483-sensors-%s-analytics-%s_%s_avg-%s_new,%d,%.2f",
                            sid, sid, sensor, dataType, windowEnd, avg));
                        out.collect(String.format("i483-sensors-%s-analytics-%s_%s_min-%s_new,%d,%.2f",
                            sid, sid, sensor, dataType, windowEnd, min));
                        out.collect(String.format("i483-sensors-%s-analytics-%s_%s_max-%s_new,%d,%.2f",
                            sid, sid, sensor, dataType, windowEnd, max));
                    }
                });

        String sid = "s2410083";

        DataStream<String> avgStream = analyticsStream
            .filter(s -> s.contains("_avg-"));
        avgStream.sinkTo(
            KafkaSink.<String>builder()
                .setBootstrapServers("150.65.230.59:9092")
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.builder()
                        .setTopicSelector((String element) -> {
                            System.out.println("[DEBUG] avgStream element: " + element.split(",", 3)[0]);
                            return element.split(",", 3)[0].toString();
                        })
                        .setValueSerializationSchema(new SerializationSchema<String>() {
                            @Override
                            public void open(InitializationContext context) {}
                            @Override
                            public byte[] serialize(String element) {
                                String[] parts = element.split(",", 3);
                                return parts[2].getBytes(StandardCharsets.UTF_8);
                            }
                        })
                        .build()
                )
                .build());

        DataStream<String> minStream = analyticsStream
            .filter(s -> s.contains("_min-"));
        minStream.sinkTo(
            KafkaSink.<String>builder()
                .setBootstrapServers("150.65.230.59:9092")
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.builder()
                        .setTopicSelector((String element) -> {
                                System.out.println("[DEBUG] minStream element: " + element.split(",", 3)[0]);
                            return element.split(",", 3)[0].toString();
                        })
                        .setValueSerializationSchema(new SerializationSchema<String>() {
                            @Override
                            public void open(InitializationContext context) {}
                            @Override
                            public byte[] serialize(String element) {
                                String[] parts = element.split(",", 3);
                                return parts[2].getBytes(StandardCharsets.UTF_8);
                            }
                        })
                        .build()
                )
                .build());

        DataStream<String> maxStream = analyticsStream
            .filter(s -> s.contains("_max-"));
        maxStream.sinkTo(
            KafkaSink.<String>builder()
                .setBootstrapServers("150.65.230.59:9092")
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.builder()
                        .setTopicSelector((String element) -> {
                            return element.split(",", 3)[0].toString();
                        })
                        .setValueSerializationSchema(new SerializationSchema<String>() {
                            @Override
                            public void open(InitializationContext context) {}
                            @Override
                            public byte[] serialize(String element) {
                                String[] parts = element.split(",", 3);
                                return parts[2].getBytes(StandardCharsets.UTF_8);
                            }
                        })
                        .build()
                )
                .build());

        DataStream<Tuple5<String, String, String, Long, Double>> parsedStream =
            env.fromSource(ks,
                WatermarkStrategy.<String>forMonotonousTimestamps()
                    .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.split(",",3)[1]))
                    .withIdleness(Duration.ofSeconds(5)),
                "CEP Source")
            .map(new LineToTupleMapper())
            .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.DOUBLE));

        Pattern<Tuple5<String, String, String, Long, Double>, ?> proximityPattern = org.apache.flink.cep.pattern.Pattern
            .<Tuple5<String, String, String, Long, Double>>begin("first")
            .where(new SimpleCondition<Tuple5<String,String,String,Long,Double>>() {
                @Override
                public boolean filter(Tuple5<String, String, String, Long, Double> value) {
                    return value.f1.equals("proximity") && value.f4 > 5.0;
                }
            })
            .next("second")
            .where(new SimpleCondition<Tuple5<String,String,String,Long,Double>>() {
                @Override
                public boolean filter(Tuple5<String, String, String, Long, Double> value) {
                    return value.f1.equals("proximity") && value.f4 > 5.0;
                }
            })
            .within(Time.seconds(10));

        org.apache.flink.cep.PatternStream<Tuple5<String, String, String, Long, Double>> patternStream =
            CEP.pattern(parsedStream.keyBy(e -> e.f2), proximityPattern);

        DataStream<String> alerts = patternStream.select(
            new PatternSelectFunction<Tuple5<String,String,String,Long,Double>, String>() {
                @Override
                public String select(java.util.Map<String, List<Tuple5<String, String, String, Long, Double>>> match) {
                    Tuple5<String, String, String, Long, Double> first = match.get("first").get(0);
                    return String.format("ALERT: student %s detected occupancy at %d", first.f2, first.f3);
                }
            });

        alerts.print().name("CEP-Alerts");

        env.execute("Flink Java Job Template");
    }

    public static class LineToTupleMapper implements org.apache.flink.api.common.functions.MapFunction<String, Tuple5<String, String, String, Long, Double>> {
        @Override
        public Tuple5<String, String, String, Long, Double> map(String line) {
            String[] parts = line.split(",", 3);
            String topic = parts[0];
            long timestamp = Long.parseLong(parts[1]);
            double value = Double.parseDouble(parts[2]);

            String[] topicParts = topic.split("-");
            String studentId = topicParts[2];
            String sensor = topicParts[3].toUpperCase();
            String dataType = topicParts[4];

            return Tuple5.of(sensor, dataType, studentId, timestamp, value);
        }
    }
}

class SensorKeySelector implements org.apache.flink.api.java.functions.KeySelector<
            Tuple5<String, String, String, Long, Double>, String> {
    @Override
    public String getKey(Tuple5<String, String, String, Long, Double> value) {
        return value.f0 + "-" + value.f1;
    }
}