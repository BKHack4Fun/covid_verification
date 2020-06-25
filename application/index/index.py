'''
for api of app
'''

from flask import Blueprint, request, render_template, url_for, redirect
from flask import jsonify

index_api = Blueprint('index_api', __name__,
                      template_folder='../templates',
                      static_folder='../static',
                      static_url_path='/static_img'
                      )


@index_api.route("/", methods=['GET', 'POST'])
def home():
    return "Hello world"