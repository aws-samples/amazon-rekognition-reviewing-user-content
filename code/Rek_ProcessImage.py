from __future__ import print_function

import boto3
from decimal import Decimal
import json
import urllib
import os

print('Loading function')

rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')

# --------------- Helper Functions to call Rekognition APIs ------------------


def create_collections():

    doesImageListExist = False

    # Get all the collections
    response = rekognition.list_collections(MaxResults=100)

    for collectionId in response['CollectionIds']:
       if(collectionId == 'ImageList'):
            doesImageListExist = True

    # Create a collection to store and Index faces
    if not doesImageListExist:
        print('Creating collection : ImageList')
        rekognition.create_collection(CollectionId='ImageList')

    return None

# --------------- Main handler ------------------


def lambda_handler(event, context):

    
    # Get the object from the event
    bucket = event['Params']['Bucket']
    key = urllib.unquote_plus(event['Params']['Key'].encode('utf8'))
    returnResult = {
        'Stage' : 'ProcessImage',
        'Pass': True,
        'ErrorMessages': []
    }
    try:

        # try:
        #     # delete collections.
        #     rekognition.delete_collection(CollectionId ='ImageList')
        # except Exception as e: 
        #     print ('Error deleting collections.')
        
        # Create image collections.
        create_collections()

        
        print('Adding image to collection')
        response = rekognition.index_faces(CollectionId='ImageList', Image={"S3Object": {
            "Bucket": bucket, "Name": key}})
        returnResult['ErrorMessages'].append('Image added to collection.')
           
        # process overall result
        event['OverallResult']['Details'].append(returnResult)
        event['OverallResult']['Pass'] = event['OverallResult']['Pass'] and returnResult['Pass'] 
        if (returnResult['Pass'] is False):
            event['OverallResult']['Reason'] = 'IMAGE_NOT_ADDED'
        else:
            event['OverallResult']['Reason'] = ''
        # Print response to console.
        # print(response)

        return event
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
