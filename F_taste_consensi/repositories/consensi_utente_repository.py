from F_taste_consensi.models.consensi_utente import ConsensiUtenteModel
from F_taste_consensi.models.log_consensi import LOGConsensi
from F_taste_consensi.db import get_session
from sqlalchemy.exc import SQLAlchemyError

class ConsensiUtenteRepository:

    @staticmethod
    def find_consensi_by_paziente_id(id_paziente, session=None):
        session = session or get_session('patient')
        return session.query(ConsensiUtenteModel).filter_by(fk_paziente=id_paziente).first()



    @staticmethod
    def save_consensi(consensi_utente, session=None):
        session = session or get_session('patient')
        session.add(consensi_utente)
        session.commit()

    @staticmethod
    def add_log_consensi(tipologia, valore, id_paziente, session=None):
        session = session or get_session('patient')
        log_consensi = LOGConsensi(tipologia=tipologia, id_paziente=id_paziente, valore=valore)
        if log_consensi:
            session.add(log_consensi)
            session.commit()


    @staticmethod
    def update_consensi(consensi_paziente, updated_data, session=None):
        session = session or get_session('patient')
        try:
            if consensi_paziente:
                for key, value in updated_data.items():
                    setattr(consensi_paziente, key, value)
                session.commit()
        except SQLAlchemyError:
            session.rollback()  


    @staticmethod
    def get_log_consensi(session=None):
        session = session or get_session('patient')
        return session.query(LOGConsensi).all()