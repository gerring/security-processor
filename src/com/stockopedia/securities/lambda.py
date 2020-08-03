'''
Created on 3 Aug 2020

@author: Matthew Gerring

TODO Run out of time with this class, it is not ready yet!

'''

import os
import boto3
import s3fs
import json

from com.stockopedia.securities.engine import Transformer

input_archive_folder = "input_archive"
to_process_folder = "to_process"
file_row_limit = 50
file_delimiter = ','

# S3 bucket info
s3 = s3fs.S3FileSystem(anon=False)
dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000") # TODO
    
def lambda_handler(event, context):
    '''
    See 
    https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-deployment-pkg.html#with-s3-example-deployment-pkg-python
    https://docs.aws.amazon.com/pinpoint/latest/developerguide/tutorials-importing-data-lambda-function-input-split.html
    
    This handler assumes that:
    1. The cloud watch lambda has stored the data in S3 files, each is a chunk which fits in memory
    2. Load the chunk of json as a list.
    3. Process all the messages in the chunk adding a DynamoDB row for each one.
    
    TODO Time was running out for this part see Transformer and the tests for the main part.
    It is possible to run handler methods from tests too and desirable as long as mocked out
    objects can replace the AWS operations like S3 and DynamoDB.
    
    '''
    
    print("Received event: \n" + str(event))
    for record in event['Records']:
        # Assign some variables that make it easier to work with the data in the 
        # event record.
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        input_file = os.path.join(bucket,key)  # TODO Not sure if path is right here for S3 events
        
        store(input_file)
    

def store(input_file):
    '''
    Store large file of many records. NOTE The ingestion will have to not store
    too many at once for this to work
    '''
    table = dynamodb.Table('Securities') # TODO is this the table we have?
    
    with s3.open(input_file, 'r') as file:
        msgs = json.load(file)
        action = lambda item : table.put_item(item)
        
        trans = Transformer()
        trans.process(msgs, action)
