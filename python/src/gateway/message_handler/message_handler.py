from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        # Un identificador unico para el Handler que maneja un cliente
        self.id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        # Envio al serializador mi ID junto con los datos
        #return message_protocol.internal.serialize([fruit, amount])
        return message_protocol.internal.serialize([self.id, [fruit, amount]])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        
        mensaje_cli_id = fields[0]
        fruit_top = fields[1]

        if mensaje_cli_id == self.id:
            return fruit_top
        return None