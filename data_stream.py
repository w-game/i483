
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Time
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext
from pyflink.datastream.window import SlidingEventTimeWindows


class MinMaxAvgFunction(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        values = [e[4] for e in elements]
        avg = sum(values) / len(values)
        min_v = min(values)
        max_v = max(values)
        out.collect(f"{key},{context.window().get_end()},{avg:.2f},{min_v:.2f},{max_v:.2f}")

def parse_line(line):
    try:
        parts = line.split(",", 2)
        topic = parts[0]
        timestamp = int(parts[1])
        value = float(parts[2])
        topic_parts = topic.split("-")
        student_id = topic_parts[2]
        sensor = topic_parts[3]
        datatype = topic_parts[4]
        return (sensor, datatype, student_id, timestamp, value)
    except Exception as e:
        print("Parse error:", e)
        return None

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

kafka_props = {
    'bootstrap.servers': '150.65.230.59:9092',
    'group.id': 'pyflink-group'
}

consumer = FlinkKafkaConsumer(
    topics='i483-sensors-s2410083-SCD41-co2',
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_props
)

raw_stream = env.add_source(consumer)

parsed_stream = raw_stream.map(
    parse_line,
    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(), Types.DOUBLE()])
)

with_timestamps = parsed_stream.assign_timestamps_and_watermarks(
    WatermarkStrategy
    .for_monotonous_timestamps()
    .with_timestamp_assigner(lambda element, record_timestamp: element[3])
)

result = with_timestamps \
    .key_by(lambda x: f"{x[0]}-{x[1]}", key_type=Types.STRING()) \
    .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10))) \
    .process(MinMaxAvgFunction(), output_type=Types.STRING())

producer = FlinkKafkaProducer(
    topic='i483-sensors-s2410083-analytics-output',
    serialization_schema=SimpleStringSchema(),
    producer_config={'bootstrap.servers': '150.65.230.59:9092'}
)

result.add_sink(producer)

env.execute("PyFlink Sensor Aggregation Job")