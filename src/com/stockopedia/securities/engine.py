'''
Created on 3 Aug 2020

@author: Matthew Gerring
'''
import calendar
import time

    

class TransformException(Exception):
    '''
    Using type name only
    '''
    pass


class Transformer(object):
    '''
    Class to transform on json message to DynamoDB row.
    '''

    def __init__(self):
        pass
    
    def process(self, list, action):
        '''
        @param list: list of data messages
        @param action: function to run on each message
        '''
        for msg in list:
            msg = self.transform(msg)
            action(msg)
    
    def transform(self, msg):
        '''
        Transform and validate message
        @param msg: something like {"security":1, "timestamp":123456789, "attribute":"sales", "value":987654321.0}
        @return row of DynamoDB data, see design for security storage.
        @raise exception: if any of the require fields are not there.
        '''
        
        self._validate(msg)
        
        # Build DynamoDB Row
        ret = {}
        ret["PK"] = msg.get('security') 
        ret["SK"] = msg.get('attribute') + "#" + str(msg.get('timestamp'))
        ret["meta"] = self._create_meta(msg)
        ret["data"] = msg
        return ret
    
    
    def _validate(self, msg):
        '''
        Validate message, throw exception if not as expected.
        @param msg: something like {"security":1, "timestamp":123456789, "attribute":"sales", "value":987654321.0}
        @raise exception: If the required fields are not present, null or empty
        '''
        if not msg:
            raise TransformException("Null message cannot be transformed or validated.")
        
        self._validate_key('security', int, msg)
        self._validate_key('timestamp', int, msg)
        self._validate_key('attribute', str, msg)
        self._validate_key('value',msg=msg)
        


    def _validate_key(self, name, clazz=None, msg=None):
        '''
        Validate key in a message, throw exception if not as expected.
        @param msg: something like {"security":1, "timestamp":123456789, "attribute":"sales", "value":987654321.0}
        @param name: name of field to check
        @raise exception: If the required field is not present, null or empty
        '''
        
        value = msg.get(name, None) # Note Null is allowed in Json, check for this in test data
        if not value:
            # Will test for None and empty
            raise TransformException("The message must contain '{}'".format(name))
        
        if clazz and not isinstance(value, clazz):
            raise TransformException("'{}' must be a {}".format(name, clazz))

    def _create_meta(self, msg):
        
        meta = {}
        meta['transform_timestamp'] = calendar.timegm(time.gmtime())
        # TODO figure out if any meta required for other features
        return meta
    