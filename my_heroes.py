from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('sqlite:///heroes_new.db', echo=True)

Base = declarative_base()

class Hero(Base):
    __tablename__ = 'heroes'

    id          = Column(Integer, primary_key=True)
    name        = Column(String)
    gender      = Column(String)
    eye_color   = Column(String)
    race        = Column(String)
    hair_color  = Column(String)
    height      = Column(Float)
    publisher   = Column(String)
    skin_color  = Column(String)
    alignment   = Column(String)
    weight      = Column(Float)

    def __repr__(self):
       base = ('Hero(id={0}, name={1}, gender={2}, eye_color={3},\n' + 
               '\trace={4}, hair_color={5}, height={6}, publisher={7},\n' + 
               '\tskin_color={8}, alignment={9}, weight={10})')
       return base.format(self.id          ,
                          self.name        ,
                          self.gender      ,
                          self.eye_color   ,
                          self.race        ,
                          self.hair_color  ,
                          self.height      ,
                          self.publisher   ,
                          self.skin_color  ,
                          self.alignment   ,
                          self.weight)

Base.metadata.create_all(engine)