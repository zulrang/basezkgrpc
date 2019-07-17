import json
import random
import time
import logging
import sys

from base_grpc import BaseZKgRPC

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)


class Calculator(BaseZKgRPC):

    ZK_CONNECTION = '127.0.0.1:2181'
    CA_FILE = 'server.crt'
    ZK_KEY = '/services/calculator'
    STUB_CLASS = 'calculator_pb2_grpc.CalculatorStub'

    METHODS = [
        # method name, request class
        ('SquareRoot', 'calculator_pb2.Number'),
        ('Add', 'calculator_pb2.NumberPair')
    ]


def main():
    calculator = Calculator()
    max = 100000
    start_time = time.time()
    total = 0.0
    for x in range(0, max):
        if x % 1000 == 0:
            root.info('On call #{}'.format(x))
        if random.randint(0, 1) == 0:
            total += calculator.SquareRoot(value=random.randint(1, 100000)).value
        else:
            total += calculator.Add(x=1, y=random.randint(1, 100000))# .value
        print("Total: {}".format(total))
    # et voilà
    print("{} calls per sec".format(max / (time.time() - start_time)))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
