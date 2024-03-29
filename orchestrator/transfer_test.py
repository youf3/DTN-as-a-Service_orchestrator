from flask import Flask
from flask_testing import TestCase
import os
import app
from test import OrchestratorTest
import unittest

class TransferTest(TestCase):

    def create_app(self):
        app.app.config['TESTING'] = True
        app.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
        app.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        return app.app

    def setUp(self):
        if os.path.exists('orchestrator/test.db'):
            os.remove('orchestrator/test.db')
        from app import db
        db.create_all()

    def tearDown(self):
        os.remove('orchestrator/test.db')
        
    def test_nuttcp_transfer(self):
        data = {
            'name' : 'testDTN1',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 1}

        data = {
            'name' : 'testDTN2',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 2}

        data = {            
            'srcfile' : ['hello_world'],
            'dstfile' : ['hello_world2'],
        }

        response = self.client.get('/ping/1/2')
        result = response.get_json()['latency']
        assert result is not None

        response = self.client.post('/transfer/nuttcp/1/2',json=data)
        result = response.get_json()
        assert result == {'result' : True, 'transfer' : 1}

        response = self.client.post('/wait/1')
        assert response.status_code == 200

        response = self.client.get('/transfer/1')
        result = response.get_json()
        assert result['id'] == 1
        assert result['sender'] == 1
        assert result['receiver'] == 2
        assert result['transfer_size'] == 13
        assert result['num_workers'] == 1

        response = self.client.get('/transfer/nuttcp')
        result = response.get_json()
        assert result['1']        
        assert result['1']['sender'] == 1
        assert result['1']['receiver'] == 2
        assert result['1']['transfer_size'] == 13
        assert result['1']['num_workers'] == 1

        response = self.client.delete('/transfer/1')
        result = response.get_json()
        assert result['id'] == 1

    def test_nuttcp_mem_transfer(self):
        data = {
            'name' : 'testDTN1',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 1}

        data = {
            'name' : 'testDTN2',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 2}

        data = {            
            'srcfile' : [None],
            'dstfile' : [None],
            'duration' : 5
        }

        response = self.client.get('/ping/1/2')
        result = response.get_json()['latency']
        assert result is not None

        response = self.client.post('/transfer/nuttcp/1/2',json=data)
        result = response.get_json()
        assert result == {'result' : True, 'transfer' : 1}

        response = self.client.post('/wait/1')
        assert response.status_code == 200

        response = self.client.get('/transfer/1')
        result = response.get_json()
        assert result['id'] == 1
        assert result['sender'] == 1
        assert result['receiver'] == 2
        assert result['transfer_size'] == 0
        assert result['num_workers'] == 1

        response = self.client.get('/transfer/nuttcp')
        result = response.get_json()
        assert result['1']        
        assert result['1']['sender'] == 1
        assert result['1']['receiver'] == 2
        assert result['1']['transfer_size'] == 0
        assert result['1']['num_workers'] == 1

        response = self.client.delete('/transfer/1')
        result = response.get_json()
        assert result['id'] == 1

    def test_nuttcp_mem_and_disk_transfer(self):
        data = {
            'name' : 'testDTN1',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 1}

        data = {
            'name' : 'testDTN2',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 2}

        data = {            
            'srcfile' : [None],
            'dstfile' : [None],
            'duration' : 5
        }

        response = self.client.get('/ping/1/2')
        result = response.get_json()['latency']
        assert result is not None

        response = self.client.post('/transfer/nuttcp/1/2',json=data)
        result = response.get_json()
        assert result == {'result' : True, 'transfer' : 1}

        data = {            
            'srcfile' : ['hello_world', 'hello_world3'],
            'dstfile' : ['hello_world2', 'hello-world4'],
        }

        response = self.client.post('/transfer/nuttcp/1/2',json=data)
        result = response.get_json()
        assert result == {'result' : True, 'transfer' : 2}

        response = self.client.post('/wait/1')
        assert response.status_code == 200

        response = self.client.post('/wait/2')
        assert response.status_code == 200

        response = self.client.get('/transfer/1')
        result = response.get_json()
        assert result['id'] == 1
        assert result['sender'] == 1
        assert result['receiver'] == 2
        assert result['transfer_size'] == 0
        assert result['num_workers'] == 1

        response = self.client.get('/transfer/nuttcp')
        result = response.get_json()
        assert result['1']        
        assert result['1']['sender'] == 1
        assert result['1']['receiver'] == 2
        assert result['1']['transfer_size'] == 0
        assert result['1']['num_workers'] == 1        

        response = self.client.get('/check/2')
        result = response.get_json()
        assert result['Unfinished'] == 0
        
        response = self.client.get('/transfer/2')
        result = response.get_json()
        assert result['id'] == 2
        assert result['sender'] == 1
        assert result['receiver'] == 2
        assert result['transfer_size'] == 26
        assert result['num_workers'] == 2
        

    def test_multiple_nuttcp_transfer(self):
        data = {
            'name' : 'testDTN1',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 1}

        data = {
            'name' : 'testDTN2',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 2}

        data = {            
            'srcfile' : ['hello_world', 'hello_world3'],
            'dstfile' : ['hello_world2', 'hello-world4'],
        }

        response = self.client.post('/transfer/nuttcp/1/2',json=data)
        result = response.get_json()
        assert result == {'result' : True, 'transfer' : 1}

        response = self.client.get('/running')
        result = response.get_json()
        assert result == [1]

        response = self.client.get('/check/1')
        result = response.get_json()
        assert 'Finished' in result
        assert 'Unfinished' in result

        response = self.client.post('/wait/1')
        assert response.status_code == 200

        response = self.client.get('/check/1')
        result = response.get_json()
        assert result['Unfinished'] == 0
        
        response = self.client.get('/transfer/1')
        result = response.get_json()
        assert result['id'] == 1
        assert result['sender'] == 1
        assert result['receiver'] == 2
        #assert result['transfer_size'] == 26
        assert result['num_workers'] == 2

        response = self.client.delete('/transfer/all')        
        assert response.status_code == 200

    def test_nuttcp_timout(self):
        data = {
            'name' : 'testDTN1',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 1}

        data = {
            'name' : 'testDTN2',
            'man_addr' : '172.17.0.1:7001',
            'data_addr' : 'localhost',
            'username' : 'nobody',
            'interface' : 'eth0'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 2}

        data = {            
            'srcfile' : ['hello_world'],
            'dstfile' : ['hello_world2'],
            'timeout' : 0
        }
        
        response = self.client.post('/transfer/nuttcp/2/1',json=data)
        result = response.get_json()
        assert result == {'result' : True, 'transfer' : 1}

        response = self.client.post('/wait/1')
        result = response.get_json()
        assert response.status_code == 200        
        assert result['failed'] == ['hello_world']


if __name__ == '__main__':
    unittest.main(verbosity=2)