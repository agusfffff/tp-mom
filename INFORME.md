# Decisiones de Diseño en MOM (Go)

Resumen de las configuraciones del MOM y sus razones de ser 

## Cola 

- Cola durable y mensaje persistente -> los mensajes pendientes sobreviven a un restart del broker y por estas razones no se pierden mensajes

- Fair dispatch -> un consumer no recibe el siguiente mensaje hasta enviar el ack del anterior (por como lo configure). De esta manera, el broker reparte segun la velocidad de cada uno, ademas los mensajes no se acumulan en cola del consumer

- autoAck=false -> si el consumer muere antes de ackear, el mensaje vuelve a la cola 

- varias instancias de queue sobre el mismo nombre de cola compiten por mensajes 

## Exchange 

- exchange direct y cola por consumer -> cada StartConsuming crea su propia cola, bindeada a las routing keys. Cada consumidor de una key recibe su propia copia del mensaje

- exclusive=true y autoDelete=true -> la cola muere sola cuando su
  unico consumer se cancela
  
- prefetchMsg de 10 -> aca no hay reparto entre consumers, cada uno tiene su cola, la idea seria que la constante de prefetch tenga sentido en el contexto de lo que se resuelve y que no explote la memoria del consumer 


## Uso

Se asume una instancia de middleware por hilo, no se comparte canal conexion entre hilos 
Lo que si se soporta es: StartConsuming → StopConsuming → StartConsuming 
Para lograr eso: 
- StartConsuming lanza una goroutine que procesa mensajes y cierra signal cuando termina 
- StopConsuming hace Cancel y bloquea en <-signal y despues retorna, así no hay race condition entre un Close y Start (nuevo)

