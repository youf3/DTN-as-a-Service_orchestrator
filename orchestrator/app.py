import requests
import datetime
import os 
from flask import Flask, request, jsonify, abort, make_response, json#, session
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import sqlalchemy
import logging
import traceback
import itertools
import concurrent.futures
import libs.ThreadExecutor

logging.getLogger().setLevel(logging.DEBUG)

app = Flask(__name__)
app.secret_key = os.urandom(16)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db/dtn.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)
thread_executor_pools = {}

gparams = {'blocksize' : 64}

with app.app_context():
    if db.engine.url.drivername == 'sqlite':
        migrate.init_app(app, db, render_as_batch=True)
    else:
        migrate.init_app(app, db)
    from flask_migrate import upgrade as _upgrade
    #_upgrade(directory='orchestrator/migrations')

class DTN(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(15), unique=False, nullable=True)
    man_addr = db.Column(db.String(80), unique=False, nullable=True)
    data_addr = db.Column(db.String(80), unique=False, nullable=True)
    username = db.Column(db.String(80), unique=False, nullable=True)
    interface = db.Column(db.String(15), unique=False, nullable=True)

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
    worker_type_id = db.Column(db.Integer, db.ForeignKey('WorkerType.id'), nullable=True)

    def __repr__(self):
        return '<Transfer %r>' % self.id

class WorkerType(db.Model):
    __tablename__ = 'WorkerType'
    id = db.Column(db.Integer, primary_key=True)
    description = db.Column(db.String(80), unique=False, nullable=True)

    def __repr__(self):
        return '<WorkerType %r>' % self.description

db.create_all()

def init_db():
    from libs.Schemes import NumaScheme    
    worker_types = {i.name:i.value for i in NumaScheme}
    wtypes = WorkerType.query.all()

    for k,v in worker_types.items():
        if k not in [i.description for i in wtypes]:
            wtype = WorkerType(id=v, description=k)
            db.session.add(wtype)

    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        #traceback.print_exc()
        abort(make_response(jsonify(message="Unable to add DTN"), 400))        

def transfer_job(sender, sender_data_ip, receiver, srcfile, dstfile, tool, data):
    result = run_transfer(sender, sender_data_ip, receiver, srcfile, dstfile, tool, data)    
    end_time = wait_for_transfer(sender, receiver, tool, result)
    return result, end_time
    #return None, None

def run_transfer(sender_ip, sender_data_ip, receiver_ip, srcfile, dstfile, tool, params):        
    global gparams

    try:        
        # logging.debug('Running sender')
        ## sender
        params['file'] = srcfile
        params['blocksize'] = gparams['blocksize']
        response = requests.post('http://{}/sender/{}'.format(sender_ip, tool), json=params)
        result = response.json()        
        if response.status_code == 404 and 'message' in result:
            abort(make_response(jsonify(message=result['message']), 404))
        if response.status_code != 200 or result.pop('result') != True:
            abort(make_response(jsonify(message="Unable start sender"), 400))
        file_size = result['size']

        ## receiver
        # logging.debug('Running Receiver')
        result['address'] = sender_data_ip
        result['file'] = dstfile
        result['blocksize'] = gparams['blocksize']
        
        response = requests.post('http://{}/receiver/{}'.format(receiver_ip, tool), json=result)
        result = response.json()
        result['dstfile'] = dstfile
        result['size'] = file_size
        
        if response.status_code != 200 or result.pop('result') != True:
            abort(make_response(jsonify(message="Unable start receiver"), 400))
    except requests.exceptions.ConnectionError:
        abort(make_response(jsonify(message="Unable to connect to DTN"), 503))
    return result

def wait_for_transfer(sender_ip, receiver_ip, tool, transfer_param):

    transfer_param['node'] = 'receiver'    
    response = requests.get('http://{}/{}/poll'.format(receiver_ip, tool), json=transfer_param)
    if not (response.status_code == 200 and response.json()[0] == 0):
        abort(make_response(jsonify(message="Transfer has failed"), 400))

    transfer_param['node'] = 'sender'    
    response = requests.get('http://{}/{}/poll'.format(sender_ip, tool), json=transfer_param)
    if not (response.status_code == 200 and response.json() == 0):
        abort(make_response(jsonify(message="Transfer has failed"), 400))
    return datetime.datetime.utcnow()

@app.route('/DTN/<int:id>')
def get_DTN(id):
    target_DTN = DTN.query.get_or_404(id)
    return {'id': target_DTN.id, 'name' : target_DTN.name, 'man_addr': target_DTN.man_addr, 'data_addr' : target_DTN.data_addr, 'username' : target_DTN.username, 'interface' : target_DTN.interface}

@app.route('/DTN/',  methods=['POST'])
def add_DTN():
    data = request.get_json()
    new_DTN = DTN(name = data['name'], man_addr = data['man_addr'], data_addr = data['data_addr'], username = data['username'], interface = data['interface'])
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
    if transfer.end_time == None: 
        abort(make_response(jsonify(message="Need to wait for the transfer id %s" % transfer_id), 400))
    data = {
        'id' : transfer.id,
        'sender' : transfer.sender_id,
        'receiver' : transfer.receiver_id,
        'start_time' : transfer.start_time.timestamp(),
        'end_time' : transfer.end_time.timestamp(),
        'transfer_size' : transfer.file_size,
        'num_workers' : transfer.num_workers,
        'num_files' : transfer.num_files,
        'latency' : transfer.latency,
        'worker_type_id' : transfer.worker_type_id
    }
    return jsonify(data)    

@app.route('/transfer/<string:tool>', methods=['GET'])
def get_transfer_for_tool(tool):
    transfers = Transfer.query.filter_by(tool=tool).all()
    data = {}
    for transfer in transfers:
        if transfer.end_time == None: continue
        data[transfer.id] = {
        'sender' : transfer.sender_id,
        'receiver' : transfer.receiver_id,
        'start_time' : transfer.start_time.timestamp(),
        'end_time' : transfer.end_time.timestamp(),
        'transfer_size' : transfer.file_size,
        'num_workers' : transfer.num_workers,
        'num_files' : transfer.num_files,
        'latency' : transfer.latency,
        'worker_type_id' : transfer.worker_type_id
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

@app.route('/worker_type',  methods=['GET'])
def get_worker_types():
    worker_types = WorkerType.query.all()
    data = {}    
    for i in worker_types:
        data[i.id] = i.description
    return jsonify(data)

@app.route('/ping/<int:sender_id>/<int:receiver_id>', methods=['get'])
def get_latency(sender_id, receiver_id):
    sender = DTN.query.get_or_404(sender_id)    
    receiver = DTN.query.get_or_404(receiver_id)
    response = requests.get('http://{}/ping/{}'.format(sender.man_addr, receiver.data_addr))    
    return response.json()

@app.route('/transfer/<string:tool>/<int:sender_id>/<int:receiver_id>', methods=['POST'])
def transfer(tool,sender_id, receiver_id):
    global thread_executor_pools
    global gparams
    
    data = request.get_json()
    srcfiles = data.pop('srcfile')
    dstfiles = data.pop('dstfile')
    sender = DTN.query.get_or_404(sender_id)
    receiver = DTN.query.get_or_404(receiver_id)
    if 'numa_scheme' in data:
        worker_type = data['numa_scheme']
    else:
        worker_type = 1

    resultset = []

    if type(srcfiles) != list:
        abort(make_response(jsonify(message="Malformed source file list"), 400))
    if type(dstfiles) != list:
        abort(make_response(jsonify(message="Malformed destionation file list"), 400))
    if len(srcfiles) != len(dstfiles):
        abort(make_response(jsonify(message="Source and destination file sizes are not matching"), 400))
    
    if 'num_workers' in data:
        if type(data['num_workers']) != int or data['num_workers'] <= 0:
            abort(make_response(jsonify(message="num_workers should be int larger than 0"), 400))                           
        else:  
            num_workers = data['num_workers']                   
    else:
        num_workers = len(srcfiles)
        data['num_workers'] = num_workers

    if 'blocksize' in data:
        try: 
            logging.debug('Setting blocksize to %s' % data['blocksize'])
            gparams['blocksize'] = int(data['blocksize'])
        except Exception:
            abort(make_response(jsonify(message="blocksize should be integer"), 400))

    latency = get_latency(sender.id, receiver.id)['latency']
    start_time = datetime.datetime.utcnow()
    new_transfer = Transfer(sender_id = sender.id, receiver_id = receiver.id, start_time = datetime.datetime.utcnow(), end_time = None, 
    file_size = None, num_files = len(srcfiles), tool=tool, num_workers = num_workers, latency = latency, worker_type_id = worker_type)    
    db.session.add(new_transfer)
    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        traceback.print_exc()
        abort(make_response(jsonify(message="Unable to log transfer"), 400))   

    start_time = datetime.datetime.utcnow()
    executor = libs.ThreadExecutor.ThreadPoolExecutor(max_workers=num_workers)
    
    future_to_transfer = {
        executor.submit(transfer_job, sender.man_addr, sender.data_addr, receiver.man_addr, srcfile, dstfile, tool, data): 
        (srcfile,dstfile) for srcfile,dstfile in zip(srcfiles, dstfiles)
        }
    thread_executor_pools[new_transfer.id] = (executor, future_to_transfer)
    return jsonify({'result' : True, 'transfer' : new_transfer.id})

@app.route('/wait/<int:transfer_id>', methods=['POST'])
def wait(transfer_id):
    global thread_executor_pools
    transfer = Transfer.query.get_or_404(transfer_id)

    file_size = 0
    end_time = None

    executor, future_to_transfer =  thread_executor_pools[transfer_id]
    for future in concurrent.futures.as_completed(future_to_transfer):
        srcfile,dstfile = future_to_transfer[future]
        try:
            result, t_end_time = future.result()            
        except Exception as exc:
            logging.debug('%r generated an exception: %s' % (srcfile, exc))
        else:
            file_size += result['size']
            if end_time == None or end_time < t_end_time: 
                end_time = t_end_time            

    executor.shutdown()
    del thread_executor_pools[transfer_id]
        
    transfer.file_size = file_size
    transfer.end_time = end_time
    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        traceback.print_exc()
        abort(make_response(jsonify(message="Unable to update transfer"), 400))   
    
    return jsonify({'result' : True})

@app.route('/running', methods=['GET'])
def get_running_transfer():
    transfers = []
    for i in thread_executor_pools:
        transfers.append(i)
    return jsonify(transfers)

@app.route('/transfer/<int:transfer_id>/scale/', methods=['POST'])
def scale_transfer(transfer_id):    
    global thread_executor_pools
    global gparams
    data = request.get_json()
    if 'num_workers' not in data and 'blocksize' not in data: 
        abort(make_response(jsonify(message="num_workers or blocksize is required"), 400))
        
    if 'num_workers' in data:    
        try: 
            num_workers = int(data['num_workers'])
        except Exception:
            abort(make_response(jsonify(message="num_workers should be integer"), 400))
        
        logging.debug('Setting num_workers to %s' % num_workers)
        executor, _ = thread_executor_pools[transfer_id]
        executor.set_max_workers(num_workers)
    
    if 'blocksize' in data:
        try: 
            blocksize = int(data['blocksize'])
        except Exception:
            abort(make_response(jsonify(message="blocksize should be integer"), 400))

        logging.debug('Setting blocksize to %s' % blocksize)
        gparams['blocksize'] = blocksize

    return ''

if __name__ == '__main__':
    init_db()
    app.run('0.0.0.0')
    pass