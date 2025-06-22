from kafka import KafkaProducer
import pandas as pd

### read data line one by one 
with open('~/data/opt/Malware-Project/BigDataset/IoTScenarios/CTU-Honeypot-Capture-4-1/bro/conn.log.labeled') as f:
    for line in f:
        line=str.encode(line)
        producer = KafkaProducer(bootstrap_servers='b-1.xxxxxxxxx.amazonaws.com:9092')
        producer.send('MSKTestTopic', line)
        producer.flush()
