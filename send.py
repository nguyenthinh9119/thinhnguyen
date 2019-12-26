import logging
import sys
import pika
import time
import smpplib.gsm
import smpplib.client
import smpplib.consts
import smpplib.pdu
import logging.config
from datetime import datetime
from logging.config import fileConfig
from logging.handlers import TimedRotatingFileHandler
import logging.handlers as handlers
import traceback
from multiprocessing import Pool, TimeoutError
import os
import threading
import queue


logging.basicConfig(filename='example.log', level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger('tcpserver')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logHandler = handlers.TimedRotatingFileHandler('example2.log', when='s', interval=20, backupCount=2)

logHandler.setFormatter(formatter)

logger.addHandler(logHandler)

logger = logging.getLogger('tcpserver')

d = {'clientip': '58.187.9.173', 'user': 'hugo'}

client = smpplib.client.Client('58.187.9.173', 2222)

# Print when obtain message_id
client.set_message_sent_handler(
    lambda pdu: sys.stdout.write('sent {} {}\n'.format(pdu.sequence, pdu.message_id))
)

client.set_message_received_handler(
    lambda pdu: sys.stdout.write('delivered {}\n'.format(pdu.source_addr))
)

client.connect()
client.bind_transceiver(system_id='hugo', password='ggoohu')

credentials = pika.PlainCredentials('guest', 'guest')

parameters = pika.ConnectionParameters('58.187.9.173', 5672, '/', credentials)
                                       
connection = pika.BlockingConnection(parameters)

channel = connection.channel()


channel.queue_declare(queue='queue_mt', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

# Crate queue
msg_q = queue.Queue()
# Python create mutex
msg_mutex = threading.Lock()

class message:
    def __init__(self,name,ch):
        self.ch = ch
        
    def callback(self,ch,method,properties,body):
        # Python mutex global
        global msg_mutex
        global msg_q
        parts, encoding_flag, msg_type_flag = smpplib.gsm.make_parts(u'{}\n'.format(body))
        print(" [x] Received %r" % body)
        # keep waiting until the lock is released
        # msg_mutex.acquire()
        msg_q.put((ch,method,properties,body))
        logger.info("Received: {}".format(body))
        time.sleep(body.count(b'.'))
        time.sleep(0.1)
        msg_mutex.release()
        print(" [x] Done")
        self.ch.basic_ack(delivery_tag=method.delivery_tag)
        for part in parts:
            pdu = client.send_message(
                    source_addr_ton=smpplib.consts.SMPP_TON_INTL,
                    source_addr='sender',
                    est_addr_ton=smpplib.consts.SMPP_TON_INTL,
                    # Make sure thease two params are byte strings, not unicode:
                    destination_addr='sdt',
                    short_message=part,
                    data_coding=encoding_flag,
                    esm_class=msg_type_flag,
                    registered_delivery=True,
            )
            print(pdu.sequence)


def worker(callback):  
    print ('Worker : %s' %callback)
    while True:
        msg_mutex.acquire()
        msg = msg_q.get()
        print ('[w-%s] msg: %s' % (callback, msg[3]))
        # msg_mutex.release()
        time.sleep(0.1)


def channel_get_msg(callback):
    #try:
        print("......Start......")
        loop = message("okok",channel)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='queue_mt', on_message_callback=loop.callback)
        channel.start_consuming()

    # except KeyboardInterrupt as er:
    #     print("error")
    #     logger.error(er,exc_info=True)

    # finally:
    #     print("finall!!!!")
if __name__ == '__main__':
    threads = []
    for callback in range(5):
        t = threading.Thread(target=worker, args=(callback,))
        threads.append(t)
        t.start()
    t = threading.Thread(target=channel_get_msg, args=(callback,))
    threads.append(t)
    t.start()
    t.join()
try:
    print("Process in Here")
except KeyboardInterrupt as er:
    print("error")
    logger.error(er,exc_info=True)

finally:
    print("finall!!!!")
