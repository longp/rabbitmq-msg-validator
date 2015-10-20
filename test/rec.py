# receive message from our daemon and print it

import pika

inputId = pika.credentials.PlainCredentials('receiver', 'receiver')

inputConnection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1',
                                                                    port=5672,
                                                                    credentials=inputId))
inputChannel = inputConnection.channel()

def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)

inputChannel.basic_consume(callback, queue='bad_msg', no_ack=True)
inputChannel.start_consuming()
