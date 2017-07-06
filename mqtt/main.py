#!/usr/bin/env python3
# -*- coding: utf8 -*-

import threading
import logging
import time

from paho.mqtt.client import Client

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger()


def pub_topic_test01(client):
    while True:
        client.publish(topic="test01", qos=2, payload="test01 topic data")
        time.sleep(2)


def pub_topic_test02(client):
    while True:
        client.publish(topic="test02", qos=2, payload="test02 topic data")
        time.sleep(2)

class MyMQTTClass(Client):
    """
    mqtt client for deal data
    """

    def __init__(self):
        super(MyMQTTClass, self).__init__(client_id="test client", clean_session=False)

    def on_connect(self, client, obj, flags, rc):
        logger.info("on connect, rc: %s, flags: %s" % (rc, flags))

        # 链接过后先处理sub
        client.subscribe(topic="test01", qos=2)
        client.subscribe(topic="test02", qos=2)

        logger.info("start topic service1...")
        t1 = threading.Thread(target=pub_topic_test01, args=(client,))
        t1.start()
        self.worker1 = t1

        logger.info("start topic service2...")
        t2 = threading.Thread(target=pub_topic_test02, args=(client,))
        t2.start()
        self.worker2 = t2

    def on_message(self, client, obj, msg):
        logger.debug("on message, topic: %s, qos: %s, data: %s" % (msg.topic, msg.qos, msg.payload))

        if msg.topic == "test01":
            logger.info("deal test01, data: %s" % msg.payload)

        elif msg.topic == "test02":
            logger.info("deal test02, data: %s" % msg.payload)

        else:
            logger.info("other topic %s, data: %s" %(msg.topic, msg.payload))

    def on_publish(self, client, obj, mid):
        logger.debug("publish -> ,mid: %s" % mid)

    def on_subscribe(self, client, obj, mid, granted_qos):
        logger.debug("subscribed <- ,mid: %s, qos: %s" %(mid, granted_qos))

    def on_log(self, mqttc, obj, level, string):
        logger.debug("mqtt debug: %s, %s" % (level, string))

    def on_disconnect(self, client, userdata, rc):
        logger.info("disconnect: %s" % rc)

        while rc == 1:
            try:
                client.reconnect()
                logger.info("reconnect success")
                rc = 0
            except Exception as e:
                logger.error("reconnect error, %s retry after 3s" % e)
                time.sleep(3)


    def run(self):
        self.connect("172.16.112.251", 1883, 60)

        while True:
            rc = self.loop()
            if rc != 0:
                time.sleep(1)
                rc = self.loop()
                logger.info("recovery from error loop, %s" % rc)



def main():
    client = MyMQTTClass()
    client.run()



if __name__ == "__main__":
    main()
