from pyflink.common import WatermarkStrategy, Types, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.table import StreamTableEnvironment, DataTypes
import json

def flink_kafka_to_iceberg():
    # 创建 Stream 执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 创建 Table 环境
    t_env = StreamTableEnvironment.create(env)
    
    # 配置 checkpoint
    env.enable_checkpointing(5000)
    
    # 配置 Catalog
    t_env.execute_sql("""
        CREATE CATALOG glue_catalog WITH (
            'type'='iceberg',
            'warehouse'='s3://<your bucket>/iceberge_warehouse/',
            'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
            'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'
        )
    """)
    
    # 使用 Glue Catalog
    t_env.use_catalog("glue_catalog")
    t_env.use_database("msk_flink_iceberge")

    # 先尝试删除已存在的临时表
    try:
        t_env.execute_sql("DROP TABLE IF EXISTS kafka_source_temp")
    except:
        pass

    # 创建临时 Kafka source table
    t_env.execute_sql("""
        CREATE TEMPORARY TABLE kafka_source_temp (
            id INT,
            data STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'MSKTestTopic',
            'properties.bootstrap.servers' = 'b-3.xxxxxxx.kafka.eu-west-1.amazonaws.com:9092',
            'properties.group.id' = 'test_group',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)
    
    # 插入数据到 Iceberg 表
    t_env.execute_sql("""
        INSERT INTO `glue_catalog`.`msk_flink_iceberge`.`sample`
        SELECT id, data FROM kafka_source_temp
    """).wait()

if __name__ == "__main__":
    flink_kafka_to_iceberg()
