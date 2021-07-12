# -*- coding: utf-8 -*-
"""
Created on Sun Jul 4 15:39:55 2021

@author: daniel
"""

import unittest
import yaml
import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


class TestMethods(unittest.TestCase):
    
    # initializing custom variables needed
    def __init__(self):
        
        # read database credentials
        self.parent_dir = os.path.dirname(os.getcwd())
        self.conf = yaml.load(open(self.parent_dir + '/credentials.yaml'), Loader=yaml.FullLoader)
        self.username = self.conf['username']
        self.password = self.conf['password']
        
        # initialize database connection 
        # method 1: sqlalchemy
        self.engine = create_engine("postgresql://{}:{}@sample.com:5432/sample_db")
        
        # method 2: psycopg2
        self.conn = psycopg2.connect(dbname = 'sample_db',
                                     host = 'sample.com',
                                     port = 5432,
                                     user = self.username,
                                     password = self.password)

        # get sample data from db (can either pull entire dataset here or within each test depending on resource & data size)
        query = """SELECT * FROM sample_schema.sample_table;"""
        self.data = pd.read_sql(query, self.engine)

    # some basic test string methods
    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)
            
    # create test for data from DB
    def test_count(self):
        self.assertEqual(len(self.data), 24998)
    
    def test_average_num_orders(self):
        self.assertEqual(self.data.order.mean(), 100)
    
    def test_num_distinct_clients(self):
        self.assertEqual(self.data.client.nunique(), 1351)


if __name__ == '__main__':
    unittest.main()