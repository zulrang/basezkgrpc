from concurrent import futures
import time
import socket
import json
import os

from kazoo.client import KazooClient
import grpc

# import the generated classes
import calculator_pb2
import calculator_pb2_grpc

# import the original calculator.py
import calculator

LISTEN_PORT = int(os.environ.get('LISTEN_PORT', 50051))
ZK_HOST = '127.0.0.1:2181'
ZK_KEY = '/services/calculator/'
CERTFILE = 'server.crt'
KEYFILE = 'server.key'


# create a class to define the server functions, derived from
# calculator_pb2_grpc.CalculatorServicer
class CalculatorServicer(calculator_pb2_grpc.CalculatorServicer):

    # calculator.square_root is exposed here
    # the request and response are of the data type
    # calculator_pb2.Number
    def SquareRoot(self, request, context):
        response = calculator_pb2.Number()
        response.value = calculator.square_root(request.value)
        return response

    def Add(self, request, context):
        response = calculator_pb2.Number()
        response.value = calculator.add(request.x, request.y)
        return response


def run_server():

    with open(KEYFILE, 'rb') as f:
        private_key = f.read()
    with open(CERTFILE, 'rb') as f:
        certificate_chain = f.read()

    server_credentials = grpc.ssl_server_credentials(
        ((private_key, certificate_chain,),)
    )

    # create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # use the generated function `add_CalculatorServicer_to_server`
    # to add the defined class to the server
    calculator_pb2_grpc.add_CalculatorServicer_to_server(
            CalculatorServicer(), server)

    # listen on port
    print('Starting server. Listening on port {}.'.format(LISTEN_PORT))
    server.add_secure_port('[::]:{}'.format(LISTEN_PORT), server_credentials)
    server.start()

    # since server.start() will not block,
    # a sleep-loop is added to keep alive
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    zk = KazooClient(hosts=ZK_HOST)
    zk.start()
    zk.create(
        ZK_KEY,
        json.dumps({
            'host': socket.gethostname(),
            'port': LISTEN_PORT
        }).encode('utf-8'),
        makepath=True, ephemeral=True, sequence=True)
    run_server()
    zk.stop()
