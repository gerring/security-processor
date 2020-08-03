'''
Created on 3 Aug 2020

@author: Matthew Gerring
'''
import unittest
import json

from com.stockopedia.securities.engine import Transformer, TransformException


class Test(unittest.TestCase):


    def setUp(self):
        self._trans = Transformer()


    def tearDown(self):
        self._trans = None


    def testSimpleSuccess(self):
        message = '{"security":1, "timestamp":123456789, "attribute":"sales", "value":987654321.0}'
        msg = json.loads(message)
        row = self._trans.transform(msg)
        assert "PK" in row
        assert "SK" in row
        assert "meta" in row
        assert "data" in row
    
    def testNone(self):
        with self.assertRaises(TransformException):
            self._trans.transform(None)

    def testNoneInJson(self):
        message = '{"security":null, "timestamp":123456789, "attribute":"sales", "value":987654321.0}'
        with self.assertRaises(TransformException):
            self._trans.transform(json.loads(message))
            
    def testSecType(self):
        message = '{"security":"Hello World", "timestamp":123456789, "attribute":"sales", "value":987654321.0}'
        with self.assertRaises(TransformException):
            self._trans.transform(json.loads(message))

    def testTimeType(self):
        message = '{"security":1, "timestamp":"BAD", "attribute":"sales", "value":987654321.0}'
        with self.assertRaises(TransformException):
            self._trans.transform(json.loads(message))
   
    def testNoneEachParam(self):
        
        message = '{"security":1, "timestamp":123456789, "attribute":"sales", "value":987654321.0}'
        msg = json.loads(message)
        
        for name in msg.keys():
            
            orig = msg.get(name)
            msg[name] = None
            try :
                with self.assertRaises(TransformException):
                    self._trans.transform(msg)
            finally:
                msg[name] = orig
                
    ''' 
    TODO  
    0. Many tests required.
    1. Check process method of list of items.
    2. More validation around timestamps being legal. 
    3. Check security code against list of supported codes?
    4. Write lambda for DynamoDB queries, check speed.
    '''
              

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()