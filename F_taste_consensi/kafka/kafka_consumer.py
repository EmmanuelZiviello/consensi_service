import os
from kafka import KafkaConsumer
import json
from F_taste_consensi.services.consensi_utente_service import ConsensiUtenteService
from F_taste_consensi.kafka.kafka_producer import send_kafka_message
# Percorso assoluto alla cartella dei certificati
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Ottiene la cartella dove si trova questo script
CERTS_DIR = os.path.join(BASE_DIR, "..", "certs")  # Risale di un livello e accede alla cartella "certs"

# Configurazione Kafka su Aiven e sui topic
KAFKA_BROKER_URL = "kafka-ftaste-kafka-ftaste.j.aivencloud.com:11837"




consumer = KafkaConsumer(
    'consensi.add.request',
    'consensi.getCondivisione.request',
    bootstrap_servers=KAFKA_BROKER_URL,
    client_id="consensi_consumer",
    group_id="consensi_service",
    security_protocol="SSL",
    ssl_cafile=os.path.join(CERTS_DIR, "ca.pem"),  # Percorso del certificato CA
    ssl_certfile=os.path.join(CERTS_DIR, "service.cert"),  # Percorso del certificato client
    ssl_keyfile=os.path.join(CERTS_DIR, "service.key"),  # Percorso della chiave privata
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

def consume(app):
    #Ascolta Kafka e chiama il Service per la registrazione
    with app.app_context():
        for message in consumer:
            data = message.value
            topic=message.topic
            if topic ==  "consensi.add.request":
                response, status = ConsensiUtenteService.add_consensi(data)  # Chiama il Service
                topic_producer = "consensi.add.success" if status == 200 else "consensi.add.failed"
                #send_kafka_message(topic_producer, response)
                #non si manda una risposta kafka dato che si vogliono solo aggiungere i consensi per l'utente senza che esso debba aspettare una risposta
            elif topic == "consensi.getCondivisione.request":
                response,status=ConsensiUtenteService.get_condivisione(data)
                topic_producer="consensi.getCondivisione.success" if status == 200 else "consensi.getCondivisione.failed"
                send_kafka_message(topic_producer,response)
            
            