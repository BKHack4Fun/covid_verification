"""
Module for the configurations of system
<<<<<<< HEAD
"""


class Neo4jConfig:
    bolt = "bolt://localhost:7687"
    username = "thanh"
    password = "thanh"

class Config:
    HOST = '0.0.0.0'
    PORT = 8081


class ElasticSearchConfig:
    ELASTICSEARCH_IP_ADDRESS = 'localhost'
    ELASTICSEARCH_PORT = 9200
    INDEX_COVID = 'covid_cases'



class NLPConfig:
    model_path = "C:/Users/HoangNam/Documents/code/xproject/covid_verification/nlp/model/"
    data_path = "C:/Users/HoangNam/Documents/code/xproject/covid_verification/covid_verification/nlp/data/"

