"""
Module for the configurations of system
"""


class Neo4jConfig:
    bolt = "bolt://localhost:11003"
    username = "thanh"
    password = "thanh"

class Config:
    HOST = '0.0.0.0'
    PORT = 8081


class ElasticSearchConfig:
    ELASTICSEARCH_IP_ADDRESS = 'localhost'
    ELASTICSEARCH_PORT = 9200
    INDEX_COVID = 'covid_cases'
