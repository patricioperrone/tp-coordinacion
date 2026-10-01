import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        # Diccionario por cliente
        self.client_tops = {}
        self.aggregator_counters = {}

        # Registrar la SIGTERM
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("SIGTERM received. Shutting down")
        self.input_queue.stop_consuming()
        try:
            self.input_queue.close()
            self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing middleware: {e}")

    def _process_data(self, cli_id, partial_top):
        logging.info(f"Received partial top from an aggregator for client {cli_id}")

        # Inicializo para clientes nuevos
        if cli_id not in self.aggregator_counters:
            self.aggregator_counters[cli_id] = 0
            self.client_tops[cli_id] = {}

        # Aumento el contador de este cliente
        self.aggregator_counters[cli_id] += 1

        # Merge de la lista parcial
        for fruit, amount in partial_top:
            actual = self.client_tops[cli_id].get(fruit, 0)
            self.client_tops[cli_id][fruit] = actual + int(amount)

        if self.aggregator_counters[cli_id] == AGGREGATION_AMOUNT:
            logging.info(f"All aggregators replied for {cli_id}. Calculating FINAL Top...")
            
            # TOP
            final_top = sorted(
                self.client_tops[cli_id].items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            final_top = final_top[:TOP_SIZE]
            
            # Enviar al Gateway
            self.output_queue.send(
                message_protocol.internal.serialize([cli_id, final_top])
            )
            
            del self.aggregator_counters[cli_id]
            del self.client_tops[cli_id]

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        cli_id = fields[0]
        partial_top = fields[1]
        
        self._process_data(cli_id, partial_top)
        
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()