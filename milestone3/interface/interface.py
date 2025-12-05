from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return render_template('interface.html')

@app.route('/api/search', methods=['GET'])
def search_solr():
    keyword = request.args.get('q')
    
    if not keyword:
        return jsonify({'error': 'Query parameter "q" is required'}), 400

    solr_url = "http://localhost:8983/solr/diseases/select"
    params = {
        'q': keyword,
        'df': 'name',
        'wt': 'json',
        'fl': 'id,name,symptoms,description',
        'rows': 10
    }

    try:
        response = requests.get(solr_url, params=params)
        return jsonify(response.json())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)