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
            'man_addr' : '127.0.0.1:8000',
            'data_addr' : '127.0.0.1',
            'username' : 'nobody'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 1}

        data = {
            'name' : 'testDTN2',
            'man_addr' : 'localhost:8000',
            'data_addr' : 'localhost',
            'username' : 'nobody'
        }

        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 2}

        data = {
            'sender' : 1,
            'receiver' : 2,
            'port' : 5001,
            'file' : 'hello_world',
            'dport' : 6001
        }

        response = self.client.post('/transfer/nuttcp',json=data)
        result = response.get_json()
        assert result == {'result' : True}

if __name__ == '__main__':
    unittest.main(verbosity=2)