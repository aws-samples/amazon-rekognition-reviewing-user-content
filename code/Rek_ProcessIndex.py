from __future__ import print_function

import boto3
import datetime
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests
from requests_aws4auth import AWS4Auth

import json
import os

s3 = boto3.client('s3')

print('Loading function')

es_conn_string = os.environ['ES_ENDPOINT']

indexDoc = {
    "dataRecord": {
        "properties": {
            "createdDate": {
                "type": "date",
                "format": "dateOptionalTime"
            },
            "objectKey": {
                "type": "string"
            },
            "objectBucket": {
                "type": "string"
            },
            "overallResult": {
                "type": "string"
            },
            "reason": {
                "type": "string"
            }
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}


def connectES(esEndPoint):
    print ('Connecting to the ES Endpoint {0}...'.format(esEndPoint))
    try:
        # print (esEndPoint)
        # Use AWS4Auth for signing the requst to ES. This is required since the Lambda function is operating under an IAM role.
        # please note, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION and AWS_SESSION_TOKEN environment variables are available to all Lambda functions.
        auth = AWS4Auth(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'],
                        os.environ['AWS_REGION'], 'es', session_token=os.environ['AWS_SESSION_TOKEN'])
        esClient = Elasticsearch(
            hosts=[{'host': esEndPoint, 'port': 443}],
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            http_auth=auth)
        print('Connection successful.')
        return esClient
    except Exception as E:
        print("Unable to connect to {0}".format(esEndPoint))
        print("Error: ", E)
        return None
        # exit(3)


def createIndex(esClient):
    try:
        print('Checking for index...')
        res = esClient.indices.exists('image-metadata-store')
        print('Index Exists: {}'.format(res))
        if res is False:
            print('Creating index image-metadata-store')
            esClient.indices.create('image-metadata-store', body=indexDoc)
        return True
    except Exception as E:
        print("Unable to Create/access Index {0}".format("image-metadata-store"))
        print("Error: ", E)
        return False
        # exit(4)


def indexDocElement(esClient, imageMetaData):
    try:
        print('Indexing document...')
        metadataBody = {
            'createdDate': datetime.datetime.now(),
            'objectKey': imageMetaData['Params']['Key'],
            'objectBucket': imageMetaData['Params']['Bucket'],
            'overallResult': imageMetaData['OverallResult']['Pass'],
            'reason': imageMetaData['OverallResult']['Reason']}
        #print (metadataBody)
        esClient.index(
            index='image-metadata-store', doc_type='images', body=metadataBody)
        print('Document Indexed successfully.')
        return True
    except Exception as E:
        print("Document not indexed")
        print("Error: ", E)
        return False
        # exit(5)


def lambda_handler(event, context):

    #print("Received event: " + json.dumps(event, indent=2))
    #print (event)
    
    try:
        esClient = connectES(es_conn_string)
        if esClient is not None:
            if createIndex(esClient) is True:
                return indexDocElement(esClient, event)
            else:
                return False
        else:
             return False
    except Exception as e:
        print(e)
        print("Error: ", e)
        raise e
