import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # Frutas y cantidadess para cada cliente
        self.client_fruits = {}

        # Contador de EoF para cada cliente
        self.eof_counters = {}

        # Registrar la SIGTERM
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("SIGTERM received. Shutting down")
        self.input_exchange.stop_consuming()
        try:
            self.input_exchange.close()
            self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing middleware: {e}")

    def _process_data(self, cli_id, fruit, amount):
        logging.info("Processing data message")
        
        # Si el cliente no existe, lo agrego
        if cli_id not in self.client_fruits:
            self.client_fruits[cli_id] = {}

        client_fruit_tops = self.client_fruits[cli_id]

        client_fruit_tops[fruit] = client_fruit_tops.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, cli_id):
        logging.info(f"Received EOF from: {cli_id}")

        # inicializo un contador de EoF para el cliente
        if cli_id not in self.eof_counters:
            self.eof_counters[cli_id] = 0

        self.eof_counters[cli_id] += 1

        # Si recibi un EoF de cada sumador
        if self.eof_counters[cli_id] == SUM_AMOUNT:
            logging.info(f"All EOFs received for {cli_id}. Calculating Top.")
            # Top de frutas desordenado de ese cliente
            unordered_fruit_top = self.client_fruits.pop(cli_id, {})

            # Ordenar el top
            fruit_top = sorted(unordered_fruit_top.values(), reverse=True)  

            # Recortar el top a la cantidad pedida por parametro
            fruit_top = fruit_top[:TOP_SIZE]
            fruit_top_tuples = [
                (item.fruit, item.amount) for item in fruit_top
            ]
            
            # Enviar al joiner
            self.output_queue.send(
                message_protocol.internal.serialize([cli_id, fruit_top_tuples])
            )

            del self.eof_counters[cli_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        cli_id = fields[0]
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(cli_id)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
