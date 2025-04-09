A. Crear carpeta kafka-logs

b. En archivo de carpeta config SERVER.PROPERTIES agregar en seccion log basic
	log.dirs=/kafka/kafka-logs

2.// CARGAR ARCHIVO PROPIEDADES
& ".\kafka-storage.bat" format --standalone -t $KAFKA_CLUSTER_ID -c "C:\kafka\config\server.properties"

3.// INICIAR SERVIDOR
.\bin\windows\kafka-server-start.bat .\config\server.properties

4. // CREAR UN TOPICO 
.\bin\windows\kafka-topics.bat --create --topic {topic-name} --bootstrap-server {host}:9092

5.// LISTAR TOPICOS
.\bin\windows\kafka-topics.bat --list --bootstrap-server 127.0.0.1:9092

6.//ESCUCHAR CONSUMIR MENSAJES 
.\bin\windows\kafka-console-consumer.bat --topic {nombreTopic} --bootstrap-server {host}:9092

7.// ENVIAR MENSAJES
Versión 4
./bin/windows/kafka-console-producer.bat --topic compras --bootstrap-server 127.0.0.1:9092
