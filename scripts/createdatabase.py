from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

class DatabaseCreator:
    def __init__(self):
        # Inicializa a base e o engine como atributos da instância
        self.Base = declarative_base()
        self.engine = create_engine('sqlite:///user_db.db')

    def create_tables(self):
        # Define a classe Usuario dentro do método para que ela tenha acesso a Base
        class Usuario(self.Base):
            __tablename__ = 'usuarios'

            id = Column(Integer, primary_key=True)
            nome = Column(String)
            idade = Column(Integer)
            email = Column(String)

        # Cria todas as tabelas
        self.Base.metadata.create_all(self.engine)

# Uso da classe:
creator = DatabaseCreator()
creator.create_tables()
