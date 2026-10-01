import pika
import random
import string
from .middleware import (
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
            # Guardo el nombre de la cola para interacciones posteriroes
            self.queue_name = queue_name
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
                self.channel = self.connection.channel()
                
                # Creo la cola con el nombre solicitado, durable para 
                # que la estructura de la cola se persista (no los msjs).
                self.channel.queue_declare(queue=self.queue_name, durable=True)
            except Exception as e:
                raise MessageMiddlewareMessageError(f"Error interno inicializando el middleware: {e}") from e
            
    #Envía un mensaje a la cola
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange = '',
                routing_key = self.queue_name,
                body = message,
                # Persistencia para no perder mensajes si nos caemos.
                properties = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Hubo un error en la conexion: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno al enviar el mensaje: {e}") from e

    #Se desconecta de la cola.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar la conexion: {e}") from e

    #Si se estaba consumiendo desde la cola, se detiene la escucha. 
    #Si no se estaba consumiendo de la cola, no tiene efecto, ni levanta
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        if not self.is_consuming:
            return
            
        try:
            self.channel.stop_consuming()
            self.is_consuming = False
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Error al dejar de consumir: {e}") from e


    def handle_ack(self, channel, tag):
            channel.basic_ack(delivery_tag=tag)

    def handle_nack(self, channel, tag):
        channel.basic_nack(delivery_tag=tag, requeue=False)

    def internal_callback(self, ch, method, properties, body):
        ack = lambda: self.handle_ack(ch, method.delivery_tag)
        nack = lambda: self.handle_nack(ch, method.delivery_tag)

        # Ejecuto la funcion de callback que nos paso el user
        # de la interfaz
        self.user_callback(body, ack, nack)

    def start_consuming(self, on_message_callback):
        self.user_callback = on_message_callback
        
        try:
            # maximo un msj a la vez
            # y hasta no acekear el anterior, no hay uno nuevo
            self.channel.basic_qos(prefetch_count=1)

            # El consumidor se registra en la cola, cuando llega
            # un mensaje ejecuta internal_callback, un wrapper
            # para el callback inyectado por el usuario
            self.channel.basic_consume(
                queue=self.queue_name, 
                on_message_callback=self.internal_callback, 
                auto_ack=False
            )
            
            self.is_consuming = True
            self.channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError:
            self.is_consuming = False
            raise MessageMiddlewareDisconnectedError("Conexion perdida al consumir")
        except Exception:
            self.is_consuming = False
            raise MessageMiddlewareMessageError("Error interno mientras se consumia un mensaje")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        # Guardo el nombre del exchange y las keys para interacciones posteriroes
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            
            # direct permite enrutar por claves
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
            
            # Una cola anonima, con 'exclusive' declaramos que solo la vamos a usar nosotros
            # y tiene como propiedad que se destruye al desconectarnos, liberando memoria
            # y evitando que se acumulen mensajes
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue

            
            for key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name, 
                    queue=self.queue_name, 
                    routing_key=key
                )
                
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Hubo un error en la conexion al inicializar: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno inicializando el exchange: {e}") from e

    #Envía un mensaje a todas las routing keys del exchange
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            # Enviamos el mensaje a todas las routing keys
            for key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=key,
                    body=message,
                    # para no perder mensajes si nos caemos
                    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Hubo un error en la conexion al enviar: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno al enviar el mensaje: {e}") from e

    #Se desconecta del middleware.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar la conexion: {e}") from e

    #Si se estaba consumiendo desde el exchange, se detiene la escucha. 
    #Si no se estaba consumiendo del exchange, no tiene efecto, ni levanta
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        if not self.is_consuming:
            return

        try:
            self.channel.stop_consuming()
            self.is_consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error al dejar de consumir: {e}") from e

    def handle_ack(self, channel, tag):
        channel.basic_ack(delivery_tag=tag)

    def handle_nack(self, channel, tag):
        channel.basic_nack(delivery_tag=tag, requeue=False)

    def internal_callback(self, ch, method, properties, body):
        ack = lambda: self.handle_ack(ch, method.delivery_tag)
        nack = lambda: self.handle_nack(ch, method.delivery_tag)
        
        # Ejecuto la funcion de callback que nos paso el user
        # de la interfaz
        self.user_callback(body, ack, nack)

    def start_consuming(self, on_message_callback):
        self.user_callback = on_message_callback
        
        try:
            # maximo un msj a la vez
            # y hasta no acekear el anterior, no hay uno nuevo
            self.channel.basic_qos(prefetch_count=1)
            
            # El consumidor se registra en la cola temporal, cuando llega
            # un mensaje ejecuta internal_callback, un wrapper
            # para el callback inyectado por el usuario
            self.channel.basic_consume(
                queue=self.queue_name, 
                on_message_callback=self.internal_callback, 
                auto_ack=False
            )
            
            self.is_consuming = True
            self.channel.start_consuming()
        # Atrapo por separado los errores de pika para no exponrselos
        # al user final
        except pika.exceptions.AMQPConnectionError:
            self.is_consuming = False
            raise MessageMiddlewareDisconnectedError("Conexion perdida al consumir")
        except Exception:
            self.is_consuming = False
            raise MessageMiddlewareMessageError("Error interno mientras se consumia un mensaje")