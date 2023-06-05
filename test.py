import unittest
import os
from flask import Flask, jsonify, abort, request
import flaskapi
import requests
import json
import sys

class TestFlaskApiUsingRequests(unittest.TestCase):
    def test_hello_world(self):
        response = requests.get('http://localhost:8080')
        self.assertEqual(response.json(), {'hello': 'test'})


class TestFlaskApi(unittest.TestCase):
    def setUp(self):
        self.app = flaskapi.app.test_client()

    def test_hello_world(self):
        response = self.app.get('/')
        self.assertEqual(
            json.loads(response.get_data().decode(sys.getdefaultencoding())), 
            {'hello': 'world'}
        )


if __name__ == "__main__":
    unittest.main()