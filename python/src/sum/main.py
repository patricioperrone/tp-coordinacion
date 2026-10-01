import os
import logging
import threading
import hashlib
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        # Cola para recibir data de gateway
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        # Exchange para comunicacion entre sumadores
        self.control_exchange_pub = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["eof_broadcast"]
        )
        self.control_exchange_sub = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["eof_broadcast"]
        )
        # Exchange para comunicacion con aggregators
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"])
            self.data_output_exchanges.append(data_output_exchange)

        # Diccionario de clientes    
        self.fruit_amount_by_client = {}
        
        # Locks
        self.lock = threading.Lock() # Protege diccionarios
        self.send_lock = threading.Lock() # Protege las conexiones
        
        #C ontador de eofs por cliente
        self.eof_pending = {}
        # Timers por cliente
        self.flush_timers = {}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("SIGTERM received. Shutting down")
        self.input_queue.stop_consuming()
        self.control_exchange_sub.stop_consuming()
        
        # Cancelar timers activos
        with self.lock:
            for timer in self.flush_timers.values():
                timer.cancel()
                
        try:
            self.input_queue.close()
            self.control_exchange_pub.close()
            self.control_exchange_sub.close()
            for exchange in self.data_output_exchanges:
                exchange.close()
        except Exception as e:
            logging.error(f"Error closing middleware: {e}")

    def _process_data(self, cli_id, data):
        logging.info(f"Processing data {data} from {cli_id}")
        fruit, amount = data

        with self.lock:
            if cli_id not in self.fruit_amount_by_client:
                self.fruit_amount_by_client[cli_id] = {}

            client_fruits = self.fruit_amount_by_client[cli_id]
            client_fruits[fruit] = client_fruits.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, int(amount))

            # Si nos llega un dato rezagado pero el EOF ya habia llegado antes, reseteamos el reloj
            if self.eof_pending.get(cli_id):
                self._reset_flush_timer(cli_id)

    def _schedule_eof_flush(self, cli_id):
        with self.lock:
            self.eof_pending[cli_id] = True
            self._reset_flush_timer(cli_id)

    def _reset_flush_timer(self, cli_id):
        # Si ya habia un timer corriendo, lo cancelamos
        if cli_id in self.flush_timers:
            self.flush_timers[cli_id].cancel()
            
        # Esperamos antes de confirmar el cierre
        timer = threading.Timer(1.5, self._execute_flush, args=[cli_id])
        self.flush_timers[cli_id] = timer
        timer.start()

    def _execute_flush(self, cli_id):
        logging.info(f"Executing deferred EOF flush for client {cli_id}")
        
        with self.lock:
            if cli_id in self.eof_pending:
                del self.eof_pending[cli_id]
            if cli_id in self.flush_timers:
                del self.flush_timers[cli_id]
                
            client_fruits = self.fruit_amount_by_client.pop(cli_id, {})

        # El envio a la red se hace bajo el send_lock para 
        # no mezclar tramas si dos temporizadores expiran juntos
        with self.send_lock:
            for client_fruit_item in client_fruits.values():
                composite_key = f"{cli_id}_{client_fruit_item.fruit}"
                hash_object = hashlib.md5(composite_key.encode('utf-8'))
                aggregator_index = int(hash_object.hexdigest(), 16) % AGGREGATION_AMOUNT
                
                data_msg = [cli_id, client_fruit_item.fruit, client_fruit_item.amount]
                self.data_output_exchanges[aggregator_index].send(message_protocol.internal.serialize(data_msg))

            logging.info(f"Broadcasting EOF to all aggregators for client {cli_id}")
            mensaje_eof = [cli_id, "EOF"]
            for exchange in self.data_output_exchanges:
                exchange.send(message_protocol.internal.serialize(mensaje_eof))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            self._process_data(*fields)
        else:
            cli_id = fields[0]
            logging.info(f"Gateway EOF received for {cli_id}. Broadcasting")
            self.control_exchange_pub.send(message_protocol.internal.serialize([cli_id]))
        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        cli_id = fields[0]
        # Programo el flush
        self._schedule_eof_flush(cli_id)
        ack()

    def _start_control_consumer(self):
        self.control_exchange_sub.start_consuming(self.process_control_message)
    def start(self):
        self.control_thread = threading.Thread(target=self._start_control_consumer)
        self.control_thread.start()

        self.input_queue.start_consuming(self.process_data_messsage)
        
        # Despues de la sigterm vuelve el control aca y hago join del hilo secundario
        logging.info("Waiting for control thread to exit")
        if self.control_thread.is_alive():
            self.control_thread.join()
            
        logging.info("Graceful shutdown")


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()