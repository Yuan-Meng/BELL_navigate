#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  2 13:56:29 2021

@author: jayfeng
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import os
import logging
import sys

Base = declarative_base()
logging_level = logging.DEBUG

logging_file = "etl.log"
    
def get_prod_engine():
    return create_engine(os.getenv('IQ_DB_AUTH'), echo=False, encoding='utf-8')

def get_prod_session():
    engine = get_prod_engine()
    Session = sessionmaker(bind=engine)
    s = Session()
    return s

def get_logging():
    logging.basicConfig(level=logging_level, filename=logging_file) 
    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.WARN)
    logger.addHandler(handler)
    return logger