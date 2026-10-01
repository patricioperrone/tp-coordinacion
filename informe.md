### 1. Gateway y Enrutamiento Multicliente
Para poder atender a más de un cliente a la vez, necesitamos poder identificar sus mensajes. De lo contrario, las métricas se entremezclan dentro de los nodos, dando un top final que es la suma de los datos de todos los clientes.

Para solucionarlo, implementé los siguientes mecanismos:

Identificación por UUID: Agregué un identificador único (UUID) a la serialización dentro del message_handler. Este ID nos ayuda a reconocer los mensajes que provienen del mismo origen en todo el sistema. Se que en clase se comentó que los UUID podrían colisionar entre varios sistemas del mundo, pero entiendo que en este sistema tan pequeñono seria posible.

Asociación de Sockets: Como el Gateway guarda cada instancia del handler asociada a un socket específico, podemos rastrear a qué cliente le pertenece cada flujo de datos.

Filtrado de Respuestas: Al final de una ejecución, cuando el resultado vuelve al Gateway, el handler compara si el Top recibido le pertenece a su cliente. Solo se envía la respuesta a través del socket si los identificadores coinciden, ignorando los mensajes de los demás.

### 2. Sumadores

**A) Estructuras de Datos**

El código provisto por la cátedra contemplaba un escenario simplificado con un solo nodo de cada tipo y un único cliente. Por lo tanto, el sumador original acumulaba frutas y cantidades sin distinción en un único diccionario global. Como se mencionó en el punto anterior, agregué el `cli_id` (o Client ID) a todos los mensajes para que los sumadores, agregadores y el joiner puedan distinguir a quien le pertenece cada dato.

Para poder trabajar con multiples clientes de forma concurrente, cambié el diccionario original `amount_by_fruit` por un diccionario con diccionarios anidados llamado `fruit_amount_by_client`. Su estructura es `K, V = (cliente, (fruta, cantidad))`, lo que nos permite acceder por ID de cliente a sus sumas parciales.

Además, agregué otros dos diccionarios para el control de flujo: `eof_pending` y `flush_timers`.
*   La primera estructura contiene pares `K, V = (cliente, booleano)` y es donde guardo si ya recibí el mensaje de EOF de ese cliente en particular. 
*   La segunda almacena las referencias a los contadores (timers) para cada cliente.

El mecanismo funciona de la siguiente manera: el método `_schedule_eof_flush(self, cli_id)` pone el flag en `True` al recibir el EOF por un canal secundario e inicia un timer de 1.5 segundos (un tiempo que me pareció prudencial para absorber latencias de red). Dentro del método `_process_data()`, que es el que maneja los nuevos datos que ingresan, si el booleano del temporizador ya está en `True` (es decir, ya hubo EOF) y llega un nuevo dato rezagado para ese cliente, se reinicia el timer invocando `_reset_flush_timer`. Así nos mantenemos escuchando un tiempo prudente por paquetes demorados en los buffers. Al final, cuando el reloj expira sin interrupciones, se ejecuta el envío a la red en `_execute_flush()` y se borra al cliente del diccionario para liberar memoria.

**B) Comunicación**

Como el enunciado nos dice que solo un sumador recibirá del Gateway el EOF original, necesitaba un medio de comunicación donde los sumadores puedan avisarse entre sí para publicar y extraer el EOF.

Para eso agregué dos *exchanges*: `control_exchange_sub` y `control_exchange_pub`. El primero se utiliza para escuchar en un hilo secundario si otro sumador nos envía el EOF, y el segundo lo uso para hacer un *broadcast* del EOF desde el único sumador que lo recibió del Gateway. La necesidad de tener 2 *exchanges* separados (uno para leer y otro para escribir) es porque las conexiones de RabbitMQ no son *thread-safe*, por lo que compartir un mismo socket entre distintos hilos rompería el protocolo.

**C) Threads**

El nodo opera con dos hilos:
*   **Hilo principal:** Es el que ya venía instanciado en el código original. Se encarga de consumir la cola de entrada, recibir la información de los clientes, acumularla y guardarla en memoria.
*   **Hilo secundario:** Es un nuevo hilo que instancié específicamente para quedarse escuchando en el `control_exchange_sub` a la espera de recibir la señal de EOF del resto de sumadores.

**D) Locks**

Para proteger la concurrencia entre el hilo principal, el hilo secundario y los temporizadores, utilice:
*   `self.lock`: Protege de forma exclusiva las lecturas y escrituras de los 3 diccionarios (las cuentas parciales por cliente, los timers y los EOF guardados).
*   `self.send_lock`: Lo agregue para sincronizar el acceso a las conexiones de red salientes. Evita que, si dos temporizadores de distintos clientes expiran exactamente en el mismo tiempo, intenten enviar mensajes simultáneamente por el mismo canal de RabbitMQ, lo cual generaría una colision.

**E) Coordinación (Sharding)**

Para enrutar la salida, se realiza *sharding* usando un hash MD5 con una clave compuesta por `cli_id + fruta`, la elección de MD5 esta basada en que es determinista (siempre da el mismo resultado para el mismo string base), y transforma cualquier string en un numero grande que podemos dividir en la cantidad de nodos para redireccionar de manera 'pareja'. Esto nos genera una cantidad de combinaciones posibles mucho mayor que usar solamente el `cli_id` o solamente la fruta. Al tener una alta cardinalidad, reparte mucho mejor los datos entre los Aggregators; ya que si solo usaba uno de los dos parámetros y resultaba de baja cardinalidad, dejaría ociosos a varios Aggregators. 

Como contrapartida (o *trade-off*), los datos de un mismo cliente quedan dispersos en varios Aggregators al mismo tiempo, teniendo el Joiner la responsabilidad final de unirlos.

Además, hay que notar un detalle de esta coordinación, cuando los sumadores agotan su temporizador, incluso si no tenían ninguna fruta guardada para ese cliente, de todas formas hacen un *broadcast* del mensaje de "EOF" hacia todos los Aggregators. Esto es para que los Aggregators puedan alcanzar la cuenta de su barrera de sincronización y no se queden esperando infinitamente.

**F) Graceful Shutdown**

Implemente un ciclo de vida de la forma:

1. **Conexiones:** Dentro del *handler* de la señal (`_handle_sigterm`), no cierro las conexiones por la fuerza. En su lugar, llamo a `stop_consuming()` en las colas de entrada. Esto le avisa a la librería Pika que rompa el ciclo infinito de lectura, destrabando los hilos.
2. **Timers:** En ese mismo *handler*, tomo el lock (`with self.lock`) y recorro el diccionario de `flush_timers` para hacer un `.cancel()` explícito sobre cualquier temporizador que haya quedado corriendo en segundo plano. Esto evita que un timer expire y trate de mandar mensajes por la red justo cuando el nodo se está apagando.
3. **Sincronización final:** Para evitar que el sistema operativo cuelgue al hilo secundario en medio de una operación, instancio el `control_thread` como un hilo normal (daemon = False). Cuando el `SIGTERM` destraba el hilo principal, la ejecución avanza a la línea final del método `start()`, donde se hace un `self.control_thread.join()`. Asi, el hilo principal espera que el hilo secundario termine.

### 3. Aggregators

**A) Estructuras de Datos**
`client_fruits` es un diccionario anidado con `K, V = (cliente, (fruta, cantidad))` donde, como en los sumadores, acumulo para cada cliente un top parcial por fruta.
`eof_counters` es un diccionario con `K, V = (cliente, contador)` que uso como barrera de sincronización.

**B) Barrera de Sincronización**
El diccionario `eof_counters` actúa como barrera, mantiene un conteo de los EOF recibidos para cada cliente, y compara contra la cantidad de sumadores (`SUM_AMOUNT`). Cuando se recibieron para un cliente la misma cantidad de EOF que sumadores, significa que tenemos toda la información necesaria para avanzar.

El método `_process_eof` es el encargado de la ejecución final:
1. Sumar uno al contador del cliente. Una vez que la barrera llega a `SUM_AMOUNT`, se hace `pop()` de ese cliente en el diccionario `client_fruits`, borrándolo de la memoria.
2. Ordenar las frutas de mayor a menor (`reverse=True`), y recortar la lista usando el límite que pide el enunciado (`fruit_top[:TOP_SIZE]`).
3. Mandar ese "Top Parcial" por la `output_queue` hacia el Joiner y borrar el contador de EOFs.

**C) Threads y Locks**
El nodo es *single-thread*. Toda la información (tanto los datos de frutas como los mensajes de EOF) entra secuencialmente por un único canal: el `input_exchange`. Como procesa mensaje por mensaje uno detrás del otro a través de `process_messsage`, es físicamente imposible que haya condiciones de carrera, por lo que **no son necesarios los locks**.

**D) Graceful Shutdown**
Se repite el patrón seguro del Sumador: utilizo `signal.signal(signal.SIGTERM, self._handle_sigterm)`. Dentro del manejador, se llama a `stop_consuming()` para destrabar el hilo principal y se cierran las colas limpiamente para evitar conexiones colgadas.

### 4. Joiner

**A) Estructuras de Datos**
Como hicimos *sharding* en la etapa de los sumadores, los datos de los clientes quedaron repartidos por los aggregators. Los Aggregators calcularon "Tops Parciales", y el trabajo de este nodo es juntar todos esos pedazos. 
Para esto agregué dos diccionarios:
* `client_tops`: Es un diccionario anidado con `K, V = (cliente, {fruta: cantidad})` donde voy sumando y consolidando las cantidades que llegan en las listas parciales.
* `aggregator_counters`: Es un diccionario con `K, V = (cliente, contador)` que uso como mi barrera de sincronización final.

**B) Barrera de Sincronización**
A diferencia del Aggregator que contaba los mensajes EOF de los sumadores, acá el Joiner cuenta "Tops Parciales" provenientes de los Aggregators. 

El método `_process_data` se encarga de esto:
1. Al recibir un Top Parcial, suma las cantidades al diccionario `client_tops` y le suma 1 al contador de ese cliente.
2. La barrera se rompe cuando el contador llega a `AGGREGATION_AMOUNT` (es decir, cuando me respondieron todos los agregadores que existen en el sistema). En ese momento sé que no me falta ninguna fruta por contar.
3. Agarro el diccionario consolidado, lo ordeno de mayor a menor, lo recorto al límite exacto pedido en el enunciado (`TOP_SIZE`) y lo envío a la cola de salida para que el Gateway se lo mande al cliente.
4. Finalmente, borro los datos del cliente en ambos diccionarios para liberar la memoria.

**C) Threads y Locks**
Mantuve la misma estructura que en el Aggregator: es *single-thread*. Toda la información ingresa de forma secuencial por la `INPUT_QUEUE`. 
Como el bucle de eventos procesa los mensajes uno por uno, no hay hilos compitiendo por modificar la memoria al mismo tiempo. Por este motivo no hay locks.

**D) Apagado**
Repeti el patron de apagado del resto del sistema. Capturo la señal, y dentro del handler, llamo a `stop_consuming()` para destabar el hilo y cierro las conexione.

### 5. Escalabilidad del Sistema

**Escalabilidad respecto a la cantidad de clientes:**
*   **Alta concurrencia:** Como cada nodo maneja diccionarios indexados por cliente, los datos se mantienen estrictamente independientes. Procesar cientos de clientes en simultáneo no mezcla las métricas. El diccionario crece en cantidad de claves, pero como cada vez que desplazamos los datos de un cliente al siguiente nodo (por ejemplo, de un sumador a un agregador) los extraemos y liberamos de la memoria, este escenario de consumo dememoria se mantiene controlado.
*   **Baja concurrencia:** Si tenemos muy pocos clientes (uno, por ejemplo), el sistema no desperdicia recursos. Pagamos el *trade-off* mínimo de tener que procesar la función de hash y buscar en diccionarios anidados, lo cual tiene un costo despreciable.

**Escalabilidad respecto al volumen de datos:**
*   **Grandes volúmenes:** Si los clientes envían muchísimos datos, pero la cantidad de frutas distintas es acotada, los diccionarios se mantiene de tamaño constante. Esto se debe a que no guardo cada dato recibido individualmente, sino que incremento el valor de un contador interno.
*   **Pequeños volúmenes:** Si hay muy poco volumen de datos fluyendo por la red, el mayor *trade-off* es el temporizador de inactividad de 1.5 segundos en los sumadores. Esto ralentiza el procesamiento de una rafaga corta, pero es un costo necesario para asegurar que no queden paquetes sin procesar.
