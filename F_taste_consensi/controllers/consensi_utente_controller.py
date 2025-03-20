from flask import request
from flask_jwt_extended import get_jwt_identity
from flask_restx import Resource, fields
from F_taste_consensi.services.consensi_utente_service import ConsensiUtenteService
from F_taste_consensi.schemas.consensi_utente import ConsensiUtenteSchema
from F_taste_consensi.namespaces import paziente_ns
from F_taste_consensi.utils.jwt_custom_decorators import paziente_required

consensi_utenti_put = paziente_ns.model('consensi_utente', {
    "storage_from_Google_fit": fields.Boolean(required=False),
    "storage_from_Health_kit": fields.Boolean(required=False),
    "condivisione_misurazioni_paziente": fields.Boolean(required=False),
    "management_user_consent": fields.Boolean(required=False),
    "statistic_user_consent": fields.Boolean(required=False),
    "trainingAI_user_consent": fields.Boolean(required=False)
})

class ConsensiUtente(Resource):

    
    @paziente_ns.doc('recupera i consensi utente')
    @paziente_required()
    def get(self):
        id_paziente = get_jwt_identity()
        return ConsensiUtenteService.get_consensi_utente(id_paziente)

    
    @paziente_ns.doc('modifica i consensi utente')
    @paziente_required()
    def put(self):
        id_paziente = get_jwt_identity()
        json_data = request.get_json()
        if not json_data:
            return {"message": "Dati non forniti"}, 400
        return ConsensiUtenteService.update_consensi_utente(id_paziente, json_data)