from F_taste_consensi.db import get_session
from F_taste_consensi.repositories.consensi_utente_repository import ConsensiUtenteRepository
from F_taste_consensi.kafka.kafka_producer import send_kafka_message
from F_taste_consensi.utils.kafka_helpers import wait_for_kafka_response
from F_taste_consensi.schemas.consensi_utente import ConsensiUtenteSchema
from F_taste_consensi.models.consensi_utente import ConsensiUtenteModel

possible_requests = (
    "storage_from_Google_fit",
    "storage_from_Health_kit",
    "condivisione_misurazioni_paziente",
    "management_user_consent",
    "statistic_user_consent",
    "trainingAI_user_consent"
)

consensi_schema_for_dump=ConsensiUtenteSchema(exclude=['fk_paziente'])

class ConsensiUtenteService:

    '''
    @staticmethod
    def get_consensi_utente(id_paziente):
        message={"id_paziente":id_paziente}
        send_kafka_message("patient.existGet.request",message)
        response_paziente=wait_for_kafka_response(["patient.existGet.success", "patient.existGet.failed"])
        #controlli su response_paziente
        if response_paziente is None:
            return {"message": "Errore nella comunicazione con Kafka"}, 500
        
        if response_paziente.get("status_code") == "200":
            #####
            session = get_session('patient')
            consensi_utente = ConsensiUtenteRepository.find_consensi_by_paziente_id(id_paziente, session)
            if not consensi_utente:
                return {"message":"Consensi non presenti nel database"}, 400
            output_richiesta=consensi_schema_for_dump.dump(consensi_utente), 200
            session.close()
            return output_richiesta
            #######
        elif response_paziente.get("status_code") == "400":
            return {"esito get_consensi_utente":"Dati mancanti"}, 400
        elif response_paziente.get("status_code") == "404":
            return {"esito get_consensi_utente":"Paziente non presente nel db"}, 404
            '''

    @staticmethod
    def add_consensi(s_consensi):
        if "id_paziente" not in s_consensi:
            return {"esito add_consensi":"Dati mancanti"}, 400
        session=get_session('patient')
        id_paziente=s_consensi["id_paziente"]
        consensi=ConsensiUtenteRepository.find_consensi_by_paziente_id(id_paziente,session)
        if consensi is not None:
            session.close()
            return {"esito add_consensi":"Consensi già presenti"}, 409
        consensi_utente=ConsensiUtenteModel(id_paziente)
        ConsensiUtenteRepository.save_consensi(consensi_utente,session)
        session.close()
        return {"message":"Consensi utente aggiunti con successo"}, 200
    
    @staticmethod
    def get_condivisione(s_consensi):
        if "id_paziente" not in s_consensi:
            return {"status_code":"400"}, 400
        session=get_session('patient')
        id_paziente=s_consensi["id_paziente"]
        consensi=ConsensiUtenteRepository.find_consensi_by_paziente_id(id_paziente,session)
        if consensi is None:
            session.close()
            return {"status_code":"404"}, 404
        condivisione_misurazioni_paziente=consensi.condivisione_misurazioni_paziente
        session.close()
        return {"status_code":"200", "condivisione_misurazioni_paziente":condivisione_misurazioni_paziente}, 200
        


    @staticmethod
    def get_consensi_utente(id_paziente):
        session = get_session('patient')
        consensi_utente = ConsensiUtenteRepository.find_consensi_by_paziente_id(id_paziente, session)
        if not consensi_utente:
            return {"message":"Consensi non presenti nel database"}, 400
        output_richiesta=consensi_schema_for_dump.dump(consensi_utente), 200
        session.close()
        return output_richiesta

    @staticmethod
    def update_consensi_utente(id_paziente,json_data):
        # Validazione dei dati
        for key in json_data:
            if key not in possible_requests:
                return {"message": f"Il consenso \"{key}\" non è un consenso valido."}, 400
            message={"id_paziente":id_paziente}
        send_kafka_message("patient.existGet.request",message)
        response_paziente=wait_for_kafka_response(["patient.existGet.success", "patient.existGet.failed"])
        #controlli su response_paziente
        if response_paziente is None:
            return {"message": "Errore nella comunicazione con Kafka"}, 500
        
        if response_paziente.get("status_code") == "200":
            #####
            session = get_session('patient')
            consensi_utente = ConsensiUtenteRepository.find_consensi_by_paziente_id(id_paziente, session)
            if consensi_utente is None:
                session.close()
                return {'message': 'Consensi utente non presenti nel db'}, 404
            
            ConsensiUtenteRepository.update_consensi(consensi_utente,json_data,session)
            # Aggiornamento dei consensi creando dei log consensi
            for key in possible_requests:
                if key in json_data:
                    ConsensiUtenteRepository.add_log_consensi(key, json_data[key], id_paziente, session)
            session.close()
            return {"message": "Consensi utente modificati con successo"}, 201
            #######
        elif response_paziente.get("status_code") == "400":
            return {"esito update_consensi_utente":"Dati mancanti"}, 400
        elif response_paziente.get("status_code") == "404":
            return {"esito update_consensi_utente":"Paziente non presente nel db"}, 404


    '''
    @staticmethod
    def update_consensi_utente(id_paziente, json_data):
        # Validazione dei dati
        for key in json_data:
            if key not in possible_requests:
                return {"message": f"Il consenso \"{key}\" non è un consenso valido."}, 400
        
        session = get_session(role='patient')
        paziente = PazienteRepository.find_by_id(id_paziente, session)
        if paziente is None:
            session.close()
            return {'message': 'Paziente non presente nel db'}, 404
        
        consensi_utente = ConsensiUtenteRepository.find_consensi_by_paziente_id(id_paziente, session)
        if consensi_utente is None:
            session.close()
            return {'message': 'Consensi utente non presenti nel db'}, 404
        
        consensi_utente=ConsensiUtenteRepository.update_consensi(consensi_utente,json_data,session)
        # Aggiornamento dei consensi creando dei log consensi
        for key in possible_requests:
            if key in json_data:
                ConsensiUtenteRepository.add_log_consensi(key, json_data[key], id_paziente, session)
        
        session.close()
        return {"message": "Consensi utente modificati con successo"}, 201
        '''