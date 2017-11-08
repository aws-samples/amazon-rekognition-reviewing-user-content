from __future__ import print_function

import boto3
import datetime
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection

import urllib
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
    print ('Connecting to the ES Endpoint {0}'.format(esEndPoint))
    try:
        print (esEndPoint)
        esClient = Elasticsearch(
            hosts=[{'host': esEndPoint, 'port': 443}],
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection)
        return esClient
    except Exception as E:
        print("Unable to connect to {0}".format(esEndPoint))
        print(E)
        # exit(3)


def createIndex(esClient):
    try:
        print('Checking for index...')
        res = esClient.indices.exists('image-metadata-store')
        print('Index Exists: {}'.format(res))
        if res is False:
            print('Creating index image-metadata-store')
            esClient.indices.create('image-metadata-store', body=indexDoc)
            return 1
    except Exception as E:
        print("Unable to Create Index {0}".format("image-metadata-store"))
        print(E)
        # exit(4)


def indexDocElement(esClient, imageMetaData):
    try:
        print('Indexing document')
        metadataBody = {
            'createdDate': datetime.datetime.now(),
            'objectKey': imageMetaData['Params']['Key'],
            'objectBucket': imageMetaData['Params']['Bucket'],
            'overallResult': imageMetaData['OverallResult']['Pass'],
            'reason': imageMetaData['OverallResult']['Reason']}
        print (metadataBody)
        retval = esClient.index(
            index='image-metadata-store', doc_type='images', body=metadataBody)
        print(retval)
    except Exception as E:
        print("Document not indexed")
        print("Error: ", E)
        # exit(5)


def lambda_handler(event, context):

    #print("Received event: " + json.dumps(event, indent=2))
    #print (event)

    # Get the object from the event
    bucket = event['Params']['Bucket']
    key = urllib.unquote_plus(event['Params']['Key'].encode('utf8'))

    esClient = connectES(es_conn_string)
    createIndex(esClient)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        indexDocElement(esClient, event)
        return True
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
