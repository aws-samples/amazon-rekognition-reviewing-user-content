from __future__ import print_function

import boto3
from decimal import Decimal
import json
import urllib

print('Loading function')

rekognition = boto3.client('rekognition')


# --------------- Helper Functions to call Rekognition APIs ------------------


def recognize_celebrities(bucket, key, inputParams):
    returnResult = {
        'Stage' : 'RecognizeCelebrities',
        'Pass': True,
        'ErrorMessages': []
    }
    response = rekognition.recognize_celebrities(Image={"S3Object": {"Bucket": bucket, "Name": key}})
    # look for 'person' label with confidence of 90% or more
    celebrityDetected = False
    celebrityName = ''
    for face in response['CelebrityFaces']:
        if Decimal(str(face['MatchConfidence'])) > 90:
            celebrityDetected = True
            celebrityName = face['Name']
            break
    if celebrityDetected:
        # print('Celebrity Detected: {}.'.format(celebrityDetected))
        # print('Your picture is very similar to {}.'.format(celebrityName))
        returnResult['Pass'] = False
        returnResult['ErrorMessages'].append('Your picture is very similar to {}.'.format(celebrityName))
    
    # process overall result
    inputParams['OverallResult']['Details'].append(returnResult)
    inputParams['OverallResult']['Pass'] = inputParams['OverallResult']['Pass'] and returnResult['Pass'] 
    if (returnResult['Pass'] is False):
        inputParams['OverallResult']['Reason'] = 'CELEBRITY_DETECTED'
    else:
        inputParams['OverallResult']['Reason'] = ''

    return inputParams



# --------------- Main handler ------------------


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event
    bucket = event['Params']['Bucket']
    key = urllib.unquote_plus(event['Params']['Key'].encode('utf8'))
    try:
        # Calls rekognition RecognizeCelebrities API to detect faces in S3 object
        response = recognize_celebrities(bucket, key, event)

        # Print response to console.
        #print(response)

        return response
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
