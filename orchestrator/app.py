import requests
from flask import Flask, request, jsonify, abort, make_response
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import sqlalchemy

import traceback

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///dtn.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)

class DTN(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(15), unique=False, nullable=True)
    man_addr = db.Column(db.String(80), unique=False, nullable=True)
    data_addr = db.Column(db.String(80), unique=False, nullable=True)
    username = db.Column(db.String(80), unique=False, nullable=True)

    def __repr__(self):
        return '<DTN %r>' % self.id

@app.route('/DTN/<int:id>')
def get_DTN(id):
    target_DTN = DTN.query.get(id)
    return {'id': target_DTN.id, 'name' : target_DTN.name, 'man_addr': target_DTN.man_addr, 'data_addr' : target_DTN.data_addr, 'username' : target_DTN.username}

@app.route('/DTN/',  methods=['POST'])
def add_DTN():
    data = request.get_json()
    new_DTN = DTN(name = data['name'], man_addr = data['man_addr'], data_addr = data['data_addr'], username = data['username'])
    db.session.add(new_DTN)
    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError as e:
        #traceback.print_exc()
        abort(make_response(jsonify(message="Unable to add DTN"), 400))
        
    return {'id' : new_DTN.id}

@app.route('/DTN/<int:id>',  methods=['DELETE'])
def delete_DTN(id):
    target_DTN = DTN.query.get(id)
    db.session.delete(target_DTN)
    return {'id' : target_DTN.id}

@app.route('/')
def check_running():
    return "The orchestrator is running"

@app.route('/transfer/<string:tool>/<int:sender_id>/<int:receiver_id>', methods=['POST'])
def transfer(tool,sender_id, receiver_id):
    data = request.get_json()    
    srcfile = data.pop('srcfile')
    dstfile = data.pop('dstfile')    
    sender = DTN.query.get(sender_id)    
    receiver = DTN.query.get(receiver_id)
    resultset = []    

    if type(srcfile) != list:
        abort(make_response(jsonify(message="Malformed source file list"), 400))
    if type(dstfile) != list:
        abort(make_response(jsonify(message="Malformed destionation file list"), 400))
    if len(srcfile) != len(dstfile):
        abort(make_response(jsonify(message="Source and destination file sizes are not matching"), 400))
    
    for i in range(len(srcfile)):
        result = run_transfer(sender, receiver, srcfile[i], dstfile[i], tool, data)
        resultset.append(result)
    
    for result in resultset:
        wait_for_transfer(sender, receiver, tool, result)
    return jsonify({'result' : True})

def run_transfer(sender, receiver, srcfile, dstfile, tool, params):        
    try:
        ## sender
        params['file'] = srcfile
        response = requests.post('http://{}/sender/{}'.format(sender.man_addr, tool), json=params)
        result = response.json()
        if response.status_code != 200 or result.pop('result') != True:
            abort(make_response(jsonify(message="Unable start sender"), 400))
        
        ## receiver
        result['address'] = sender.data_addr
        result['file'] = dstfile
        
        response = requests.post('http://{}/receiver/{}'.format(receiver.man_addr, tool), json=result)
        result = response.json()
        result['dstfile'] = dstfile
        if response.status_code != 200 or result.pop('result') != True:
            abort(make_response(jsonify(message="Unable start receiver"), 400))
    except requests.exceptions.ConnectionError:
        abort(make_response(jsonify(message="Unable to connect to DTN"), 503))
    return result

def wait_for_transfer(sender, receiver, tool, transfer_param):

    transfer_param['node'] = 'receiver'    
    response = requests.get('http://{}/{}/poll'.format(receiver.man_addr, tool), json=transfer_param)
    if not (response.status_code == 200 and response.json()[0] == 0):
        abort(make_response(jsonify(message="Transfer has failed"), 400))

    transfer_param['node'] = 'sender'    
    response = requests.get('http://{}/{}/poll'.format(sender.man_addr, tool), json=transfer_param)
    if not (response.status_code == 200 and response.json() == 0):
        abort(make_response(jsonify(message="Transfer has failed"), 400))

if __name__ == '__main__':
    app.run('0.0.0.0')
    pass