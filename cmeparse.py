#!/usr/bin/env python
import os
import datetime
import argparse
from sqlalchemy import create_engine, MetaData, Table, Column, Float, String, DateTime, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from lxml import objectify
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('-u','--uri',help='sqlalchemy db engine uri',default='sqlite:///cme.sqlite')
Base = declarative_base()

class BaseSecurity:
    def __str__(self):
        return ','.join(map(str,self.to_tuple()))
    
    def __repr__(self):
        return '<BaseSecurity: {}>'.format(self) 

    def __gt__(self,other):
        return self.to_tuple()>other.to_tuple()

    def __ge__(self,other):
        return self.to_tuple()>=other.to_tuple()

    def __lt__(self,other):
        return self.to_tuple()<other.to_tuple()

    def __le__(self,other):
        return self.to_tuple()<=other.to_tuple()

    def __eq__(self,other):
        return self.to_tuple()==other.to_tuple()

    def __ne__(self,other):
        return self.to_tuple()!=other.to_tuple()

    def __hash__(self):
        return hash(str(self))

    def to_tuple(self):
        return tuple(v for k,v in vars(self).items() if not k.startswith('_'))

    def to_dict(self):
        return {k:v for k,v in vars(self).items() if not k.startswith('_')}

class Underlying(BaseSecurity,Base):
    __tablename__ = 'underlying'
    exch = Column(String(10),primary_key=True,nullable=False)
    symbol = Column(String(10),primary_key=True,nullable=False)
    expiry = Column(Integer,primary_key=True,nullable=False,default=0)
    inst_type = Column(String(1),primary_key=True)
    @classmethod
    def from_xml(cls,Exch,ID,MMY=None,SecTyp=None,**kwargs):
        if MMY:
            MMY = int(MMY) if len(MMY)==8 else int(MMY+'01')
        if SecTyp:
            SecTyp = 'F' if SecTyp=='FUT' else 'S'
        return cls(exch=Exch,symbol=ID,expiry=MMY,inst_type=SecTyp)

    def to_tuple(self):
        return self.exch,self.symbol,self.expiry,self.inst_type

    def __repr__(self):
        return '<Underlying: {}>'.format(self) 

class Instrument(BaseSecurity,Base):
    __tablename__ = 'instrument'
    exch = Column(String(10),primary_key=True,nullable=False)
    symbol = Column(String(10),primary_key=True,nullable=False)
    expiry = Column(Integer,primary_key=True,nullable=False,default=0)
    maturity = Column(Integer,primary_key=True,nullable=False,default=0)
    inst_type = Column(String(1),primary_key=True)
    strike = Column(Float,primary_key=True)
    @classmethod
    def from_xml(cls,Exch,Sym,MMY=None,MatDt=None,PutCall=None,StrkPx=None,Undly=None,**kwargs):
        if MMY:
            MMY = int(MMY) if len(MMY)==8 else int(MMY+'01')
        if MatDt:
            MatDt = datetime.datetime.strptime(MatDt,'%Y-%m-%d').strftime('%Y%m%d')
            MatDt = int(MatDt)
        if StrkPx:
            StrkPx = float(StrkPx)
        if PutCall is None:
            PutCall = 'F' if MMY else 'S'
        else:
            PutCall = 'C' if 1 else 'P'
        StrkPx = float(StrkPx) if StrkPx else 0.
        inst = cls(exch=Exch,symbol=Sym,expiry=MMY,maturity=MatDt,inst_type=PutCall,strike=StrkPx)
        inst._underlying = Undly
        return inst

    @property
    def underlying(self):
        return self._underlying

    def to_tuple(self):
        return self.exch,self.symbol,self.expiry,self.maturity,self.inst_type,self.strike

    def __repr__(self):
        return '<Instrument: {}>'.format(self) 

def parse_file(filename,path='settle'):
    with open(os.path.join(path,filename),'rb') as fh:
        obj = objectify.XML(fh.read())
    data = obj.Batch.MktDataFull
    arr = []
    for i in data:
        inst = dict(i.Instrmt.attrib)
        und = Underlying.from_xml(**i.Undly.attrib) if hasattr(i,'Undly') else None
        inst['instrument'] = Instrument.from_xml(Undly=und,**inst)
        inst['underlying'] = und
        inst['date'] = datetime.datetime.strptime(i.values()[0],'%Y-%m-%d')
        for full in i.Full:
            full = dict(full.attrib)
            full.update(inst)
            arr.append(full)
    df = pd.DataFrame(arr)
    df.index = df['date']
    return df

def insert_to_db(df):
    und = df.underlying.dropna()
    und = pd.DataFrame([i.to_dict() for i in set(und)])
    inst = df.instrument.dropna()
    inst = pd.DataFrame([i.to_dict() for i in set(inst)])
    del df['underlying']
    del df['instrument']
    und.to_sql('underlying',engine,if_exists='append',index=False)
    inst.to_sql('instrument',engine,if_exists='append',index=False)
    df.to_sql('market_data',engine,if_exists='append',index=False)

def main(base='settle'):
    for filename in os.listdir(base):
        if filename.endswith('s.xml'):
            df = parse_file(filename)
            insert_to_db(df)

if __name__=='__main__':
    args = parser.parse_args()
    engine = create_engine(args.uri)
    Base.metadata.create_all(engine)
    main()
