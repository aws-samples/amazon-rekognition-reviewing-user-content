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


def add_image_to_Collection(bucket, key):

    doesBlackListImagesExist = False

    # Get all the collections
    response = rekognition.list_collections(MaxResults=100)

    for collectionId in response['CollectionIds']:
        if(collectionId == 'BlackListImages'):
            doesBlackListImagesExist = True

    # Create a blacklist collection
    if not doesBlackListImagesExist:
        #print('Creating collection : BlackListImages')
        rekognition.create_collection(CollectionId='BlackListImages')
        # Since the collection did not exist, add the existing images from the blacklist bucket, to the blacklist image collection
        #print('Adding BlackList Images')
        imageList = s3.list_objects_v2(
            Bucket=bucket, Prefix=key)
        # print(imageList)
        for image in imageList['Contents']:
            if(image['Size'] == 0):
                continue
            print('Adding ' + bucket + '/' + image['Key'])
            rekognition.index_faces(CollectionId='BlackListImages', Image={"S3Object": {
                "Bucket": bucket, "Name": image['Key']}})

    # Now add the image which fired the Lambda function.
    print('Adding ' + bucket + '/' + key)
    rekognition.index_faces(CollectionId='BlackListImages', Image={"S3Object": {
        "Bucket": bucket, "Name": key}})

    return None

# --------------- Main handler ------------------


def lambda_handler(event, context):

    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(
        event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:

        try:
            # delete collections.
            rekognition.delete_collection(CollectionId ='BlackListImages')
        except Exception as e: 
            print ('Error deleting collections.')


        # Create image collections.
        add_image_to_Collection(bucket, key)

        # Print response to console.
        # print(response)

        return True
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
