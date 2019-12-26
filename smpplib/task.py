#!/usr/bin/env python
import pika
import sys
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='58.187.9.173'))
channel = connection.channel()
n = 0
while n < 1:
        channel.queue_declare(queue='queue_mt', durable=True)


        message = {'destination_address': '2100', 'source_address': '84925338061', 'message': "hello world   %s"%n }

        channel.basic_publish(
                exchange='',
                routing_key='queue_mt',
                body=str(message),
                properties=pika.BasicProperties(
                delivery_mode=1,  # make message persistent
                ))
        print(" [x] Sent %r" % message)
        time.sleep(2)
        n+=1
connection.close()