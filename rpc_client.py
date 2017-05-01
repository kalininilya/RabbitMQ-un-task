#!/usr/bin/env python
import pika
import uuid
import msgpack


class LinAlRpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            self.on_response, no_ack=True, queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id, ),
            body=str(n))
        while self.response is None:
            self.connection.process_data_events()
        return self.response


LinAl_rpc = LinAlRpcClient()

#example request for slau
request = {"data": {}, "req_type": "slau"}
request['data']['a'] = [[3, 1], [1, 2]]
request['data']['b'] = [9, 8]
print " [x] Requesting for ", request['req_type']
request = msgpack.packb(request)
response = LinAl_rpc.call(request)
response = msgpack.unpackb(response)
print " [.] Got ", response['solution']

#example request for Cholesky decomposition
request = {"data": {}, "req_type": "cho"}
request['data']['a'] = [[3, 1], [1, 2]]
print " [x] Requesting for ", request['req_type']
request = msgpack.packb(request)
response = LinAl_rpc.call(request)
response = msgpack.unpackb(response)
print " [.] Got ", response['solution']

#example request for Singular value decomposition
request = {"data": {}, "req_type": "svd"}
request['data']['a'] = [[3, 1], [1, 2]]
print " [x] Requesting for ", request['req_type']
request = msgpack.packb(request)
response = LinAl_rpc.call(request)
response = msgpack.unpackb(response)
print " [.] Got ", response['solution']

#example request for det
request = {"data": {}, "req_type": "det"}
request['data']['a'] = [[3, 1], [1, 2]]
print " [x] Requesting for ", request['req_type']
request = msgpack.packb(request)
response = LinAl_rpc.call(request)
response = msgpack.unpackb(response)
print " [.] Got ", response['solution']
