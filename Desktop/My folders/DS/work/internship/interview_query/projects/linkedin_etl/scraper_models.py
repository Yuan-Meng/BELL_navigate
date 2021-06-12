#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 15:00:23 2021

@author: jayfeng
"""

from sqlalchemy import Sequence, Column, Integer, String, Boolean, DateTime, Float
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import func

import os
import logging
import sys
# Jupyter fix
import re
from dotenv import load_dotenv


logging_file = 'scraper_logs.log'
logging_level = logging.DEBUG

levels_scraper_id = 1
indeed_job_id = 2
lsn_job_id = 3
levels_etl_id = 4
roles_etl_id = 5

Base = declarative_base()

class RawData(Base):
    __tablename__ = 'raw_data'
    id = Column(Integer, nullable=False, primary_key=True)
    value = Column(String, nullable=False)
    attribution_id = Column(Integer, nullable=False)
    scraped_job_id = Column(Integer, nullable=False)
    search_query = Column(String, nullable=True)
    migrated = Column(Boolean, default=False)
    
class EtlLogs(Base):
    __tablename__= 'etl_logs'
    id = Column(Integer, nullable=False, primary_key=True)
    successful_run = Column(Boolean, nullable=False)
    rows_migrated = Column(Integer, nullable=False)
    job_id = Column(Integer, nullable=False)
    extra_data = Column(String, nullable=True)

class Role(Base):
    __tablename__='roles'
    id = Column(Integer, nullable=False, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    user_submitted_company = Column(String, nullable=True)
    user_submitted_title = Column(String, nullable=True)
    user_submitted_location = Column(String, nullable=True)
    company_id = Column(Integer, nullable=True)
    position_title_id = Column(Integer, nullable=True)
    city_id = Column(Integer, nullable=True)
    seniority_id = Column(Integer, nullable=True)

class Salaries(Base):
    __tablename__='salaries'
    id = Column(Integer, nullable=False, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    submitted_at = Column(DateTime, nullable=False)
    role_id = Column(Integer, nullable=False)
    level = Column(String, nullable=True)
    years_of_experience = Column(Float, nullable=True)
    years_at_company = Column(Float, nullable=True)
    base_salary = Column(Integer, nullable=True)
    bonus = Column(Integer, nullable=True)
    stock_value = Column(Integer, nullable=True)
    total_yearly_compensation = Column(Integer, nullable=True)
    attribution = Column(String, nullable=False, default="iq")
    is_verified = Column(Boolean, nullable=False, default=0)
    is_outlier = Column(Boolean, nullable=False, default=0)
    post_id = Column(Integer, nullable=True)
    attribution_id = Column(Integer, nullable=True)
    
class Profile(Base):
    __tablename__='profiles'
    id = Column(Integer, nullable=False, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    user_id = Column(String, nullable=True)
    url = Column(String, nullable=True)
    name = Column(String, nullable=True)
    headline = Column(String, nullable=True)
    summary = Column(String, nullable=True)
    location = Column(String, nullable=True)
    attribution = Column(String, nullable=True)
    attribution_id = Column(String, nullable=True)    
    industry = Column(String, nullable=True)    
    linkedin_id = Column(Integer, nullable=True) 
    
class EducationInstitution(Base):
    __tablename__='education_institutions'
    id = Column(Integer, nullable=False, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    name = Column(String, nullable=True)
    linkedin_edu_id = Column(Integer, nullable=True)    
    
class ProfileExperience(Base):
    __tablename__='profile_experiences'
    id = Column(Integer, nullable=False, primary_key=True)
    profile_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    role_id = Column(Integer, nullable=True)
    description = Column(String, nullable=True)
    from_date = Column(DateTime, nullable=True)
    to_date = Column(DateTime, nullable=True)
    most_recent = Column(Boolean, nullable=True)
    current_role = Column(Boolean, nullable=True)
    
class ProfileEducation(Base):
    __tablename__='profile_educations'
    id = Column(Integer, nullable=False, primary_key=True)
    profile_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    institution_id = Column(Integer, nullable=True)
    degree = Column(String, nullable=True)
    field = Column(String, nullable=True)
    from_date = Column(DateTime, nullable=True)
    to_date = Column(DateTime, nullable=True)
    
# def get_analytics_engine():
#     return create_engine(os.getenv('ANALYTICS_DB_AUTH'), echo=False, encoding='utf-8')

def get_analytics_engine():
    # take environment variables from .env
    load_dotenv()

    # create engine (below is a Jupyter fix)
    return create_engine(
        re.sub(r"(?<=2019)\d+", "$$", os.environ.get("ANALYTICS_DB_AUTH")),
        echo=False,
        encoding="utf-8",
    )

def get_analytics_session():
    engine = get_analytics_engine()
    Session = sessionmaker(bind=engine)
    s = Session()
    return s

# def get_prod_engine():
#     return create_engine(os.getenv('IQ_DB_AUTH'), echo=False, encoding='utf-8')

def get_prod_engine():
    # take environment variables from .env
    load_dotenv()

    # create engine (below is a Jupyter fix)
    return create_engine(
        re.sub(r"(?<=2019)\d+", "$$", os.environ.get("IQ_DB_AUTH")),
        echo=False,
        encoding="utf-8",
    )

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

def insert_data(results, num_results, scraper_id, logger, s):
    try:
        s.bulk_save_objects(results)
        s.commit()
    except Exception as e:
        msg = f"Error inserting scraper id {scraper_id} data into the analytics db: {e}"
        logger.error(msg)
        dump_analytics(s, num_results, False, scraper_id, msg)   
        sys.exit("Quitting from insert data error")

def dump_analytics(s, num_results, success, scraper_id, msg=None):
    s.add(
        EtlLogs(
            successful_run = success,
            rows_migrated = num_results,
            job_id = scraper_id
        )
    )
    s.commit()
    
def get_scraped_data_by_id(s, scraper_id):
    return (
        s.query(RawData)
        .filter(RawData.scraped_job_id == scraper_id, RawData.migrated == False)
        .all()
    )

def get_education_from_linkedin_id(s, linkedin_edu_id):
    return (
        s.query(EducationInstitution)
        .filter(EducationInstitution.linkedin_edu_id == linkedin_edu_id)
        .first()
    )

def get_existing_ids(s, scraper_id):
    query = f"""
    SELECT attribution_id 
    FROM raw_data 
    WHERE scraped_job_id = {scraper_id}
    """
    r = []
    results = s.execute(query)
    for row in results:
        r.append(row['attribution_id'])
    return r
    