#send a json string to our daemon

import pika
inputId = pika.credentials.PlainCredentials('producer','producer')
inputConnection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1',port=5672,credentials=inputId))
inputChannel = inputConnection.channel()

inputChannel.basic_publish(exchange='', routing_key='input',body='{ "firstName": "John", "lastName": "Smith", "isAlive": true}')
