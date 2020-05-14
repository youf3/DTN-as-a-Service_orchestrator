import requests
import datetime
from flask import Flask, request, jsonify, abort, make_response
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import sqlalchemy
import logging
import traceback
import itertools
import concurrent.futures

logging.getLogger().setLevel(logging.DEBUG)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db/dtn.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)

with app.app_context():
    if db.engine.url.drivername == 'sqlite':
        migrate.init_app(app, db, render_as_batch=True)
    else:
        migrate.init_app(app, db)
    from flask_migrate import upgrade as _upgrade
    _upgrade(directory='orchestrator/migrations')

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)

class DTN(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(15), unique=False, nullable=True)
    man_addr = db.Column(db.String(80), unique=False, nullable=True)
    data_addr = db.Column(db.String(80), unique=False, nullable=True)
    username = db.Column(db.String(80), unique=False, nullable=True)

    def __repr__(self):
        return '<DTN %r>' % self.id

class Transfer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sender_id = db.Column(db.Integer, db.ForeignKey('DTN.id'), nullable=True)
    receiver_id = db.Column(db.Integer, db.ForeignKey('DTN.id'), nullable=True)
    start_time = db.Column(db.DateTime, nullable=False, default=datetime.datetime.utcnow)
    end_time = db.Column(db.DateTime, nullable=True)
    file_size = db.Column(db.Integer, nullable=True, default = 0)
    num_files = db.Column(db.Integer, nullable=True)
    tool = db.Column(db.String(80), unique=False, nullable=True)
    num_workers = db.Column(db.Integer, nullable=True, default = 0)
    latency = db.Column(db.Float, nullable=True, default = 0)    

    def __repr__(self):
        return '<Transfer %r>' % self.id

@app.route('/DTN/<int:id>')
def get_DTN(id):
    target_DTN = DTN.query.get_or_404(id)
    return {'id': target_DTN.id, 'name' : target_DTN.name, 'man_addr': target_DTN.man_addr, 'data_addr' : target_DTN.data_addr, 'username' : target_DTN.username}

@app.route('/DTN/',  methods=['POST'])
def add_DTN():
    data = request.get_json()
    new_DTN = DTN(name = data['name'], man_addr = data['man_addr'], data_addr = data['data_addr'], username = data['username'])
    db.session.add(new_DTN)
    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        #traceback.print_exc()
        abort(make_response(jsonify(message="Unable to add DTN"), 400))
        
    return {'id' : new_DTN.id}

@app.route('/DTN/<int:id>',  methods=['DELETE'])
def delete_DTN(id):
    target_DTN = DTN.query.get(id)
    db.session.delete(target_DTN)
    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        #traceback.print_exc()
        abort(make_response(jsonify(message="Unable to add DTN"), 400))
    return {'id' : target_DTN.id}

@app.route('/')
def check_running():
    return "The orchestrator is running"

@app.route('/transfer/<int:transfer_id>', methods=['GET'])
def get_transfer(transfer_id):
    transfer = Transfer.query.get_or_404(transfer_id)
    data = {
        'id' : transfer.id,
        'sender' : transfer.sender_id,
        'receiver' : transfer.receiver_id,
        'start_time' : transfer.start_time.timestamp(),
        'end_time' : transfer.end_time.timestamp(),
        'transfer_size' : transfer.file_size,
        'num_workers' : transfer.num_workers,
        'num_files' : transfer.num_files,
        'latency' : transfer.latency
    }
    return jsonify(data)

@app.route('/transfer/<int:transfer_id>',  methods=['DELETE'])
def delete_transfer(transfer_id):
    transfer = Transfer.query.get_or_404(transfer_id)
    db.session.delete(transfer)
    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        #traceback.print_exc()
        abort(make_response(jsonify(message="Unable to delete transfer"), 400))
    return {'id' : transfer.id}

@app.route('/transfer/all',  methods=['DELETE'])
def delete_all_transfers():
    transfers = Transfer.query.all()
    for transfer in transfers:
        db.session.delete(transfer)
    db.session.delete(transfer)
    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        #traceback.print_exc()
        abort(make_response(jsonify(message="Unable to delete all transfers"), 400))
    return ''

def transfer_job(sender, receiver, srcfile, dstfile, tool, data):
    result = run_transfer(sender, receiver, srcfile, dstfile, tool, data)    
    wait_for_transfer(sender, receiver, tool, result)
    return result

@app.route('/transfer/<string:tool>/<int:sender_id>/<int:receiver_id>', methods=['POST'])
def transfer(tool,sender_id, receiver_id):
    data = request.get_json()    
    srcfiles = data.pop('srcfile')
    dstfiles = data.pop('dstfile')    
    sender = DTN.query.get_or_404(sender_id)    
    receiver = DTN.query.get_or_404(receiver_id)
    start_time = datetime.datetime.utcnow()
    file_size = 0 
    resultset = []    

    if type(srcfiles) != list:
        abort(make_response(jsonify(message="Malformed source file list"), 400))
    if type(dstfiles) != list:
        abort(make_response(jsonify(message="Malformed destionation file list"), 400))
    if len(srcfiles) != len(dstfiles):
        abort(make_response(jsonify(message="Source and destination file sizes are not matching"), 400))
    
    if 'num_threads' in data:
        if type(data['num_threads']) != int or data['num_threads'] <= 0:
            abort(make_response(jsonify(message="num_threads should be int larger than 0"), 400))                           
        else:  
            num_workers = data['num_threads']                   
    else:
        num_workers = len(srcfiles)

    response = requests.get('http://{}/ping/{}'.format(sender.man_addr, receiver.data_addr))
    latency = response.json()['latency']

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_transfer = {
            executor.submit(transfer_job, sender, receiver, srcfile, dstfile, tool, data): 
            (srcfile,dstfile) for srcfile,dstfile in zip(srcfiles, dstfiles)
            }
        for future in concurrent.futures.as_completed(future_to_transfer):
            srcfile,dstfile = future_to_transfer[future]
            try:
                result = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (srcfile, exc))
            else:
                file_size += result['size']                    

    end_time = datetime.datetime.utcnow()
    new_transfer = Transfer(sender_id = sender.id, receiver_id = receiver.id, start_time = start_time, end_time = end_time, 
    file_size = file_size, num_files = len(srcfiles), tool=tool ,num_workers = num_workers, latency = latency)
    db.session.add(new_transfer)
    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        traceback.print_exc()
        abort(make_response(jsonify(message="Unable to log transfer"), 400))   
    
    return jsonify({'result' : True, 'transfer' : new_transfer.id})

def run_transfer(sender, receiver, srcfile, dstfile, tool, params):        
    try:
        logging.debug('Running sender')
        ## sender
        params['file'] = srcfile
        response = requests.post('http://{}/sender/{}'.format(sender.man_addr, tool), json=params)
        result = response.json()        
        if response.status_code == 404 and 'message' in result:
            abort(make_response(jsonify(message=result['message']), 404))
        if response.status_code != 200 or result.pop('result') != True:
            abort(make_response(jsonify(message="Unable start sender"), 400))
        file_size = result['size']

        ## receiver
        logging.debug('Running Receiver')
        result['address'] = sender.data_addr
        result['file'] = dstfile
        
        response = requests.post('http://{}/receiver/{}'.format(receiver.man_addr, tool), json=result)
        result = response.json()
        result['dstfile'] = dstfile
        result['size'] = file_size
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