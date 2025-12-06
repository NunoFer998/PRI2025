import json
import os
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)


def load_systems(filename):
    with open(filename, 'r') as f:
        return json.load(f)

search_systems = {
    'basic': load_systems('./systems/basic.json'),
    'enhanced': load_systems('./systems/enhanced.json'),
    'treatment': load_systems('./systems/treatmentSort.json')
}

@app.route('/')
def home():
    return render_template('interface.html')

@app.route('/api/search', methods=['GET'])
def search_solr():
    keyword = request.args.get('q')
    mode = request.args.get('mode', 'basic') # default
    
    if not keyword:
        return jsonify({'error': 'Query parameter "q" is required'}), 400

    if mode in search_systems:
        params = search_systems[mode].copy()
    else:
        params = search_systems['basic'].copy()

    params['q'] = keyword

    if 'rows' not in params:
        params['rows'] = 10

    solr_url = "http://localhost:8983/solr/diseases/select"

    try:
        response = requests.get(solr_url, params=params)
        result = response.json()
        result['debug_mode_used'] = mode

        return jsonify(response.json())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)