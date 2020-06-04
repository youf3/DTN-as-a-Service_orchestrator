import requests
import itertools
import traceback
import time
import random

import queue
from threading import Thread

def test_large_Transfer():

    data = {            
        'srcfile' : ['disk0/data0', 'disk1/data0'],# 'disk2/data0', 'disk3/data0' ],
        'dstfile' : ['disk0/data0', 'disk1/data0'],# 'disk2/data0', 'disk3/data0'],
        'num_workers' : 1
    }

    response = requests.post('http://{}/transfer/nuttcp/2/1'.format('localhost:5000'), json=data)
    transfer_id = response.json()['transfer']
    time.sleep(30)
    response = requests.post('http://{}/transfer/{}/scale/'.format('localhost:5000', transfer_id),json={'num_workers' : 2})

    response = requests.post('http://{}/wait/{}'.format('localhost:5000', transfer_id), json=data)   
    
    result = response.json()
    assert result['result'] == True    

def test_add_DTN():

    data = {
        'name' : 'r740xd1',
        'man_addr' : '165.124.33.174:5000',
        'data_addr' : '10.250.38.58',
        'username' : 'nobody'
    }
    response = requests.post('http://{}/DTN/'.format('localhost:5000'), json=data)    
    result = response.json()
    assert result == {'id' : 1}

    data = {
        'name' : 'r740xd2',
        'man_addr' : '165.124.33.175:5000',
        'data_addr' : '10.250.38.59',
        'username' : 'nobody'
    }
    response = requests.post('http://{}/DTN/'.format('localhost:5000'), json=data)
    result = response.json()
    assert result == {'id' : 2}

def test_get_DTN():        
    response = requests.get('http://{}/DTN/1'.format('localhost:5000'))
    result = response.json()
    assert result == {'id' : 2}

def test_local_transfer():
    data = {
        'srcfile' : ['disk0/data'],
        'dstfile' : ['disk0/data'],
        'num_threads' : 1
    }

    response = requests.post('http://{}/transfer/nuttcp/2/1'.format('localhost:5000'),json=data)
    result = response.json()

#test_get_DTN()
#test_add_DTN()
#test_get_DTN()
# test_remove_transfer()

#test_random_small_Transfer()
test_large_Transfer()

