from __future__ import print_function

import boto3
from decimal import Decimal
import json
import urllib
import os

print('Loading function')

rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')

blacklist_bucket = os.environ['BLACKLIST_BUCKET']
blacklist_prefix = urllib.unquote_plus(os.environ['BLACKLIST_PREFIX'].encode('utf8')) 

# --------------- Helper Functions to call Rekognition APIs ------------------




def check_Blacklist_Duplicates(bucket, key, inputParams):

    returnResult = {
        'Stage' : 'CheckBlackList_Dups',
        'Pass': True,
        'ErrorMessages': []
    }
    
    blackListMatchDetected = False
    duplicateMatchDetected = False
    # Check in the blackList first
    response = rekognition.search_faces_by_image(
        CollectionId='BlackListImages', Image={"S3Object": {"Bucket": bucket, "Name": key}}, MaxFaces=1)
    #print ('Checking BlackList')
    # print(response)
    # Check for at least one face returned
    if (response.get('FaceMatches') is not None and len(response['FaceMatches']) > 0):
        blackListMatchDetected = True
        returnResult['Pass'] = False
        returnResult['ErrorMessages'].append('Blacklist Match Detected.')
        #print ('blackListMatchDetected = {}'.format(blackListMatchDetected))

    if not blackListMatchDetected:
        # Check in our image collection, for duplicates
        response = rekognition.search_faces_by_image(
            CollectionId='ImageList', Image={"S3Object": {"Bucket": bucket, "Name": key}}, MaxFaces=1)

        #print ('Checking Duplicates')
        # print(response)
        if (response.get('FaceMatches') is not None and response['FaceMatches']):
            duplicateMatchDetected = True
            # print ('duplicateMatchDetected = {}'.format(duplicateMatchDetected))
            returnResult['Pass'] = False
            returnResult['ErrorMessages'].append('Duplicate Image Detected.')

    # process overall result
    inputParams['OverallResult']['Details'].append(returnResult)
    inputParams['OverallResult']['Pass'] = inputParams['OverallResult']['Pass'] and returnResult['Pass'] 
    if (returnResult['Pass'] is False):
        inputParams['OverallResult']['Reason'] = 'BLACKLIST_DUPS_DETECTED'
    else:
        inputParams['OverallResult']['Reason'] = ''

    return inputParams

def create_collections():

    doesBlackListImagesExist = False
    doesImageListExist = False

    # Get all the collections
    response = rekognition.list_collections(MaxResults=100)

    for collectionId in response['CollectionIds']:
        if(collectionId == 'BlackListImages'):
            doesBlackListImagesExist = True
        if(collectionId == 'ImageList'):
            doesImageListExist = True
       
    # Create a blacklist collection
    if not doesBlackListImagesExist:
        print('Creating collection : BlackListImages...')
        rekognition.create_collection(CollectionId='BlackListImages')
        # Add BlackList Images
        print('Adding BlackList Images..')
        imageList = s3.list_objects_v2(
            Bucket=blacklist_bucket, Prefix=blacklist_prefix)
        #print(imageList)
        for image in imageList['Contents']:
            if(image['Size'] == 0):
                continue
            print('Adding ' + image['Key'])
            rekognition.index_faces(CollectionId='BlackListImages', Image={"S3Object": {
                "Bucket": blacklist_bucket, "Name": image['Key']}})

    # Create a image collection
    if not doesImageListExist:
        print('Creating collection : ImageList')
        rekognition.create_collection(CollectionId='ImageList')
    return None

# --------------- Main handler ------------------


def lambda_handler(event, context):

    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event
    bucket = event['Params']['Bucket']
    key = urllib.unquote_plus(event['Params']['Key'].encode('utf8'))
    try:

        # try:
        #     # delete collections.
        #     rekognition.delete_collection(CollectionId ='BlackListImages')
        #     rekognition.delete_collection(CollectionId ='ImageList')

        # except Exception as e: 
        #     print ('Error deleting collections.')

        # Create image collections.
        create_collections()

        response = check_Blacklist_Duplicates(bucket, key, event)

        # Print response to console.
        # print(response)

        return response
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
