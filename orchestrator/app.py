import datetime
import logging
import os
import requests
import sqlalchemy
import traceback
from flask import Flask, request, jsonify, abort, make_response, json
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate, upgrade
from threading import Lock

from libs.Schemes import NumaScheme
from libs.TransferRunner import Sender, Receiver, TransferRunner

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.getLevelName(os.environ.get('LOG_LEVEL', 'info').upper()),
    datefmt='%Y-%m-%d %H:%M:%S')

app = Flask(__name__)
app.secret_key = os.urandom(16)
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{os.getcwd()}/db/dtn.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)
transfer_runners = {}
runner_lock = Lock()

class TransferException(Exception):
    def __init__(self, msg):
        self.msg = msg        

    def __str__(self):
        return self.msg

class DTN(db.Model):
    __tablename__ = 'DTN'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(15), unique=False, nullable=True)
    man_addr = db.Column(db.String(80), unique=False, nullable=True)
    data_addr = db.Column(db.String(80), unique=False, nullable=True)
    username = db.Column(db.String(80), unique=False, nullable=True)
    interface = db.Column(db.String(15), unique=False, nullable=True)
    token = db.Column(db.String(80), unique=False, nullable=True)

    def __repr__(self):
        return '<DTN %r>' % self.id

class WorkerType(db.Model):
    __tablename__ = 'WorkerType'
    id = db.Column(db.Integer, primary_key=True)
    description = db.Column(db.String(80), unique=False, nullable=True)

    def __repr__(self):
        return '<WorkerType %r>' % self.description

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

def init_db():
    with app.app_context():
        if db.engine.url.drivername == 'sqlite':
            migrate.init_app(app, db, render_as_batch=True)
        else:
            migrate.init_app(app, db)
        #upgrade(directory='orchestrator/migrations')

        db.create_all()

        wtypes = WorkerType.query.all()
        worker_types = {i.name:i.value for i in NumaScheme}

        for k,v in worker_types.items():
            if k not in [i.description for i in wtypes]:
                wtype = WorkerType(id=v, description=k)
                db.session.add(wtype)

        try:
            db.session.commit()
        except sqlalchemy.exc.IntegrityError:
            #traceback.print_exc()
            abort(make_response(jsonify(message="Unable to add DTN"), 400))

@app.route('/DTN/<int:id>')
def get_DTN(id):
    target_DTN = DTN.query.get_or_404(id)
    return {'id': target_DTN.id, 'name' : target_DTN.name, 'man_addr': target_DTN.man_addr, 'data_addr' : target_DTN.data_addr,
            'username' : target_DTN.username, 'interface' : target_DTN.interface, 'jwt_token': target_DTN.token}

@app.route('/DTN/')
def get_DTNs():
    dtns = DTN.query.all()
    return jsonify([{"name": dtn.name, "id": dtn.id, 'man_addr': dtn.man_addr, 'data_addr' : dtn.data_addr, 'username' : dtn.username, 'interface' : dtn.interface} for dtn in dtns])

@app.route('/DTN/',  methods=['POST'])
def add_DTN():
    data = request.get_json()
    new_DTN = DTN(name = data['name'], man_addr = data['man_addr'], data_addr = data['data_addr'], username = data['username'], interface = data['interface'], token = data['jwt_token'])
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
        'worker_type_id' : transfer.worker_type_id,
        # compute some helpful statistics too
        'elapsed_time': transfer.end_time.timestamp() - transfer.start_time.timestamp(),
        'transfer_rate': (transfer.file_size / (transfer.end_time.timestamp() - transfer.start_time.timestamp())),
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
    # delete the database session
    transfer = Transfer.query.get_or_404(transfer_id)

    # clear the transfer from the runner list if active
    global transfer_runners
    with runner_lock:
        if transfer_id in transfer_runners:
            transfer_runners[transfer_id].cancel()
            transfer_runners[transfer_id].wait()
            transfer_runners[transfer_id].shutdown()
            del transfer_runners[transfer_id]

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
    # delete all transfers
    for transfer in transfers:
        db.session.delete(transfer)

    # delete all running transfers
    global transfer_runners
    with runner_lock:
        for transfer_id in transfer_runners:
            transfer_runners[transfer_id].cancel()
            transfer_runners[transfer_id].shutdown()
            del transfer_runners[transfer_id]
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

@app.route('/ping/<int:sender_id>/<int:receiver_id>', methods=['GET'])
def get_latency(sender_id, receiver_id):
    sender = DTN.query.get_or_404(sender_id)
    receiver = DTN.query.get_or_404(receiver_id)
    response = requests.get('http://{}/ping/{}'.format(sender.man_addr, receiver.data_addr),
        headers=({"Authorization": request.headers.get('Authorization')} if request.headers.get('Authorization') else None))    
    return response.json()

@app.route('/transfer/<string:tool>/<int:sender_id>/<int:receiver_id>', methods=['POST'])
def transfer(tool,sender_id, receiver_id):    
    data = request.get_json()
    srcfiles = data.pop('srcfile')
    dstfiles = data.pop('dstfile')
    sender = DTN.query.get_or_404(sender_id)
    receiver = DTN.query.get_or_404(receiver_id)
    if 'numa_scheme' in data:
        worker_type = data['numa_scheme']
    else:
        worker_type = 1

    if 'timeout' in data and type(data['timeout']) == int:
        timeout = data['timeout']
    else:
        timeout = None

    if type(srcfiles) != list:
        abort(make_response(jsonify(message="Malformed source file list"), 400))
    if type(dstfiles) != list:
        abort(make_response(jsonify(message="Malformed destination file list"), 400))
    if len(srcfiles) != len(dstfiles):
        abort(make_response(jsonify(message="Source and destination file sizes are not matching"), 400))
    
    if 'num_workers' in data:
        if type(data['num_workers']) != int or data['num_workers'] <= 0:
            abort(make_response(jsonify(message="num_workers should be int larger than 0"), 400))
        elif data['num_workers'] > 999:
            # limitation of the agent - currently only a port range of 1000 is set aside for most tools
            abort(make_response(jsonify(message="num_workers should be int smaller than 1000"), 400))
        else:
            num_workers = data['num_workers']
    else:
        num_workers = min(len(srcfiles), 999)
        data['num_workers'] = num_workers

    if 'blocksize' in data:
        try: 
            logging.debug('Setting blocksize to %s' % data['blocksize'])
            data['blocksize'] = int(data['blocksize'])
        except Exception:
            abort(make_response(jsonify(message="blocksize should be integer"), 400))

    latency = get_latency(sender.id, receiver.id)['latency']    
    new_transfer = Transfer(
        sender_id=sender.id,
        receiver_id=receiver.id,
        start_time=datetime.datetime.utcnow(),
        end_time=None, 
        file_size=None,
        num_files=len(srcfiles),
        tool=tool,
        num_workers=num_workers,
        latency=latency,
        worker_type_id=worker_type)
    db.session.add(new_transfer)
    try:
        db.session.commit()
    except sqlalchemy.exc.IntegrityError:
        traceback.print_exc()
        abort(make_response(jsonify(message="Unable to log transfer"), 400))   
    
    # authorization for sender and receiver
    sender_token = data.get('sender_token')
    receiver_token = data.get('receiver_token')

    global transfer_runners
    with runner_lock:
        transfer_runners[new_transfer.id] = TransferRunner(
            new_transfer.id,
            Sender(sender.man_addr, sender.data_addr, sender_token),
            Receiver(receiver.man_addr, receiver_token),
            srcfiles,
            dstfiles,
            tool,
            data,
            num_workers=num_workers,
            timeout=timeout
            )
    return jsonify({'result' : True, 'transfer' : new_transfer.id})

@app.route('/wait/<int:transfer_id>', methods=['POST'])
def wait(transfer_id):
    global transfer_runners
    if transfer_id not in transfer_runners:
        abort(make_response(jsonify(message="Transfer ID not found")), 404)

    with runner_lock:
        transfer = Transfer.query.get_or_404(transfer_id)
        transfer_runners[transfer_id].wait()
        transfer_runners[transfer_id].shutdown()
        transfer.file_size = transfer_runners[transfer_id].transfer_size
        transfer.end_time = transfer_runners[transfer_id].end_time
        failed_files = transfer_runners[transfer_id].failed_files
        if len(failed_files) > 0 and failed_files[0] is not None:
            failed_files = sorted(failed_files)
        try:
            db.session.commit()
            return jsonify({'result': True, 'failed': failed_files})
        except sqlalchemy.exc.IntegrityError:
            traceback.print_exc()
            abort(make_response(jsonify(message="Unable to update transfer"), 400))   
        finally:
            del transfer_runners[transfer_id]        

@app.route('/check/<int:transfer_id>', methods=['GET'])
def check(transfer_id):    
    if transfer_id not in transfer_runners:
        return {'Unfinished': 0}
    
    return jsonify(transfer_runners[transfer_id].poll())

@app.route('/running', methods=['GET'])
def get_running_transfer():
    return jsonify([id for id in transfer_runners])

@app.route('/transfer/<int:transfer_id>/scale/', methods=['POST'])
def scale_transfer(transfer_id):
    data = request.get_json()
    if 'num_workers' not in data and 'blocksize' not in data: 
        abort(make_response(jsonify(message="num_workers or blocksize is required"), 400))

    if transfer_id not in transfer_runners:
        abort(make_response(jsonify(message="Transfer ID not found")), 404)

    if 'num_workers' in data:
        try: 
            num_workers = int(data['num_workers'])
        except Exception:
            abort(make_response(jsonify(message="num_workers should be integer"), 400))
        
        logging.debug('Setting num_workers to %s' % num_workers)
        with runner_lock:
            transfer_runners[transfer_id].scale(num_workers)
    
    if 'blocksize' in data:
        # TODO fix new TransferRunner to modify blocksizes
        abort(make_response(jsonify(message="blocksize change not yet supported")))

        try: 
            blocksize = int(data['blocksize'])
        except Exception:
            abort(make_response(jsonify(message="blocksize should be integer"), 400))

        logging.debug('Setting blocksize to %s' % blocksize)
        #gparams['blocksize'] = blocksize

    return ''

init_db()

if __name__ == '__main__':
    app.run('0.0.0.0', port=5001)
