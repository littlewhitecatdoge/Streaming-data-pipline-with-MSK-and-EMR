# Streaming data pipline with MSK and EMR

This project implements real-time data processing from Kafka using Flink and Spark Streaming on Amazon EMR, with data being written to S3, Apache Iceberg, and HBase respectively.

---------
---------
## Archtecture Design

#### 1.IOT/Streaming data source -> kafka -> flink-> s3
#### 2.Simple Json data -> kafka -> flink-> iceberg
#### 3.IOT/Streaming data source -> kafka -> spark streaming -> s3
#### 4.IOT/Streaming data source -> kafka -> spark streaming -> hbase

--------
--------
## Prerequisites

### EMR Cluster
- EMR version 7.2.0 (**single master**)
- application: hadoop, flink, hive, zookeeper
- Enable iceberg:
```
[{
"Classification":"iceberg-defaults",
    "Properties":{"iceberg.enabled":"true"}
}]
```
- set up hive env in master node:
```
sudo cp /usr/lib/hive/auxlib/aws-glue-datacatalog-hive3-client.jar /usr/lib/flink/lib 
sudo cp /usr/lib/hive/lib/antlr-runtime-3.5.2.jar /usr/lib/flink/lib 
sudo cp /usr/lib/hive/lib/hive-exec-3.1.3*.jar /lib/flink/lib 
sudo cp /usr/lib/hive/lib/libfb303-0.9.3.jar /lib/flink/lib 
sudo cp /usr/lib/flink/opt/flink-connector-hive_2.12-1.15.2.jar /lib/flink/lib
sudo chmod 755 /usr/lib/flink/lib/aws-glue-datacatalog-hive3-client.jar 
sudo chmod 755 /usr/lib/flink/lib/antlr-runtime-3.5.2.jar 
sudo chmod 755 /usr/lib/flink/lib/hive-exec-3.1.3*.jar 
sudo chmod 755 /usr/lib/flink/lib/libfb303-0.9.3.jar
sudo chmod 755 /usr/lib/flink/lib/flink-connector-hive_*.jar
```
- download jar for flink:
```
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar

wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

chmod 700 flink-sql-connector-kafka-3.2.0-1.18.jar

chmod 700 kafka-clients-3.5.1.jar
```
- install python dependency
```
sudo pip3 install google-api-python-client
```


### MSK
- version 3.5.1
- enbale plaintext
- create topic:
```
#download kafka client
wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz

tar -xzf kafka_2.13-3.5.1.tgz

#create topic
cd kafka_2.13-3.5.1/bin

./kafka-topics.sh --create --bootstrap-server b-3.flinkdemocluster.79yf0q.c2.kafka.eu-west-1.amazonaws.com:9092 --replication-factor 3 --partitions 1 --topic MSKTestTopic
```

### Find dataset
- data source
```
https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/iot_23_datasets_small.tar.gz
```
- Download it to EC2 and decompress it.

```
wget https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/iot_23_datasets_small.tar.gz

tar -zxvf iot_23_datasets_small.tar.gz
```

### Make MSK & EMR & EC2 connected

- Setting the security group of MSK and EMR cluster to all traffic to their Security group:
```
all-traffice allow  <sg-name>
```
- !! notice EMR has two sg and MSK has one. All of them need to allow traffic

---------
---------
## Steps for different solutions
### 1.IOT/Streaming data source -> kafka -> flink-> s3

- Run flink job (remember setting your s3 bucket on code ```kafka_to_s3.py```)
```
# start a session
flink-yarn-session -d
# run flink job 
flink run -py kafka_to_s3.py --jarfile flink-sql-connector-kafka-3.2.0-1.18.jar --jarfile kafka-clients-3.5.0.jar
```

- Run ```steaming_data_producer.py``` on ec2

```
python3 streaming_data_producer.py 
```

- Check the sink data on s3 path  


### 2.Simple Json data -> kafka -> flink-> iceberg

- start flink session:
```
# starting the Flink YARN Session in detached mode
flink-yarn-session -d 
```
- create iceberg table in flink sql:

```
cd /usr/lib/flink/bin
./sql-client.sh
```
- SQL:
```
CREATE CATALOG glue_catalog WITH (
   'type'='iceberg',
   'warehouse'='s3://<your bucket>/iceberge_warehouse/',
   'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
   'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'
 );

USE CATALOG  glue_catalog;

CREATE DATABASE IF NOT EXISTS msk_flink_iceberge;

USE msk_flink_iceberge;

CREATE TABLE IF NOT EXISTS `glue_catalog`.`msk_flink_iceberge`.`sample` (id int, data string);
```
- submit flink code ```kafka_flink_iceberg.py```:
```
flink run -py kafka_flink_iceberg.py --jarfile flink-sql-connector-kafka-3.2.0-1.18.jar --jarfile kafka-clients-3.5.0.jar
```
- input json by simple client in ec2:
```
kafka_2.13-3.5.1/bin
/kafka-console-producer.sh \
  --broker-list b-3.flinkdemocluster.79yf0q.c2.kafka.eu-west-1.amazonaws.com:9092 \
  --topic MSKTestTopic
>{"id": 100086, "data": "test message 10086"}
```

### 3.IOT/Streaming data source -> kafka -> Spark steaming -> s3

- Submit Spark job with code ```spark_kafka_to_s3.py``` (remember to change s3 folder and MSK broker link)
```
spark-submit --master yarn --deploy-mode cluster spark_kafka_to_s3.py
```
- Run ```steaming_data_producer.py``` on ec2
```
python3 streaming_data_producer.py 
```
- check s3 path

### 4.IOT/Streaming data source -> kafka -> Spark steaming -> Hbase

- Use hbase shell to create a hbase table
```
sudo hbsae shell
create 'vehicle_data2', 'info','location'
```
- Compile spark_KafkaToHbase and spark_KafkaToHbase to a jar file

- Submit Spark job
```
spark-submit --class com.mycompany.app.KafkaStreamingToHbase xxxxxxxxxxx.jar
```

- Run ```steaming_data_producer.py``` on ec2

```
python3 streaming_data_producer.py 
```

- check hbase table
