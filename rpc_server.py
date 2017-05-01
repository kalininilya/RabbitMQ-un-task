#!/usr/bin/env python
import pika
import time
import msgpack
import numpy as np

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')


def solveSlau(body):
    a = body['data']['a']
    b = body['data']['b']
    x = np.linalg.solve(a, b)
    return x.tolist()


def cho(body):
    a = body['data']['a']
    return [
        list(np.linalg.qr(a))[0].tolist(), list(np.linalg.qr(a))[1].tolist()
    ]


def svd(body):
    a = body['data']['a']
    return [
        list(np.linalg.svd(a, full_matrices=True))[0].tolist(),
        list(np.linalg.svd(a, full_matrices=True))[1].tolist()
    ]


def det(body):
    a = body['data']['a']
    return np.linalg.det(a).tolist()


def on_request(ch, method, props, body):
    body = msgpack.unpackb(body)
    print " [.] Got request for ", body['req_type']
    if body['req_type'] == 'slau':
        body['solution'] = solveSlau(body)
        response = body
        print " [.] Request fullfiled, sending solution", body
    elif body['req_type'] == 'cho':
        body['solution'] = cho(body)
        response = body
        print " [.] Request fullfiled, sending solution", body
    elif body['req_type'] == 'qr':
        body['solution'] = qr(body)
        response = body
        print " [.] Request fullfiled, sending solution", body
    elif body['req_type'] == 'svd':
        body['solution'] = svd(body)
        response = body
        print " [.] Request fullfiled, sending solution", body
    elif body['req_type'] == 'det':
        body['solution'] = det(body)
        response = body
        print " [.] Request fullfiled, sending solution", body
    else:
        response = "Error: Unknown type of request"
        print " [.] Request is not fullfiled, sending error", body
    response = msgpack.packb(body)

    print "Sleeping..."
    time.sleep(5)
    print "Sleeping for 5 more seconds..."
    time.sleep(5)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='rpc_queue')

print " [x] Awaiting RPC requests"
channel.start_consuming()
