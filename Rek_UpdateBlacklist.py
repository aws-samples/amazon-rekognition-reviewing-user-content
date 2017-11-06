from __future__ import print_function

import json
import urllib
import boto3

print('Loading function')

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')


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
        print('Creating collection : BlackListImages')
        rekognition.create_collection(CollectionId='BlackListImages')

    # Create a collection to store and Index faces
    if not doesImageListExist:
        print('Creating collection : ImageList')
        rekognition.create_collection(CollectionId='ImageList')

    return None


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(
        event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:

         # Create image collections.
        create_collections()

         # Add BlackList Images
        response = rekognition.index_faces(CollectionId='BlackListImages', Image={"S3Object": {
            "Bucket": bucket, "Name": key}})
        print('Image {} added to Blacklist collection'.format(key))
        return response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
