from flask import Flask
from flask_testing import TestCase
import os
import app
import unittest

class OrchestratorTest(TestCase):

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

    def test_running(self):
        response = self.client.get('/')
        result = response.data        
        assert result == b'The orchestrator is running'

    def test_add_DTN(self):
        data = {
            'name' : 'testDTN1',
            'man_addr' : '192.168.1.1',
            'data_addr' : '192.168.2.1',
            'username' : 'nobody'
        }
        response = self.client.post('/DTN/',json=data)
        result = response.get_json()
        assert result == {'id' : 1}

    def test_get_DTN(self):
        self.test_add_DTN()
        response = self.client.get('/DTN/1')
        result = response.get_json()
        assert result == {
            'name' : 'testDTN1',
            'man_addr' : '192.168.1.1',
            'data_addr' : '192.168.2.1',
            'id' : 1,
            'username' : 'nobody'
        }

    def test_delete_DTN(self):
        self.test_add_DTN()
        response = self.client.delete('/DTN/1')
        result = response.get_json()
        assert result == {'id' : 1}

        response = self.client.get('/DTN/1')
        assert response.status_code == 404

    # def test_add_twice(self):
    #     self.test_add_DTN()
    #     data = {
    #         'name' : 'testDTN1',
    #         'man_addr' : '192.168.1.1',
    #         'data_addr' : '192.168.2.1',
    #         'username' : 'nobody'
    #     }
        
    #     response = self.client.post('/DTN/', json=data)
    #     result = response.get_json()
    #     assert response.status_code == 400
    #     assert result == {'message' : 'Unable to add DTN'}    

if __name__ == '__main__':
    unittest.main(verbosity=2)