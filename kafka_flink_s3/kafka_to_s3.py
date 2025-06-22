from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import Encoder,SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig,RollingPolicy
from pyflink.datastream.connectors.kafka import KafkaSource,KafkaOffsetsInitializer

def flink_kafka_to_s3():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    #配置 5 s 检查一次
    env.enable_checkpointing(5000)
    
    # 配置 Kafka 源
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("b-1.xxxxxxxx.amazonaws.com:9092") \
        .set_topics("MSKTestTopic") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 从 Kafka 消费数据
    kafka_stream = env.from_source(kafka_source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Source")


    # 配置 sink
    output_path = "s3://<yourbucket name>/kafka_output_data/"

    # 配置 rolling policy 默认是 128mb,60s,60s

    rolling_Policy=RollingPolicy.default_rolling_policy() 

    # 配置 sink
    sink = FileSink.for_row_format(output_path,Encoder.simple_string_encoder()).with_output_file_config(OutputFileConfig.builder().with_part_suffix(".txt").build()).with_rolling_policy(rolling_Policy).build()

    # 将数据写入 S3

    kafka_stream.sink_to(sink)

    # 执行作业
    env.execute("Kafka to S3")

if __name__ == "__main__":
    flink_kafka_to_s3()
