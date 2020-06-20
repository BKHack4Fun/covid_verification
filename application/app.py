'''
init app
'''

from elasticsearch import Elasticsearch
from flask import Flask
from application.index.index import index_api
from config.config import Config, ElasticSearchConfig

app = Flask(__name__)

app.register_blueprint(index_api, static_folder=None)

# elastic_search = Elasticsearch([{'host': ElasticSearchConfig.ELASTICSEARCH_IP_ADDRESS, 'port': ElasticSearchConfig.ELASTICSEARCH_PORT}])
# if not elastic_search.indices.exists(index=ElasticSearchConfig.INDEX_COVID):
#     elastic_search.indices.create(index=ElasticSearchConfig.INDEX_COVID)

if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT,threaded=False)