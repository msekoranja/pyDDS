import dds
import time

def on_data(dict):
    print(dict)

dds_instance = dds.DDS('dds_testTypes')
topic = dds_instance.get_topic('generated.dds.org.eelt.Sample')
print(topic)
msg = dict(value=3.14)
topic.publish(msg)

topic.subscribe(on_data)

while True:
    time.sleep(100)
