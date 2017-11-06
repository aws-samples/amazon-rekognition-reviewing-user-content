from __future__ import print_function

import boto3
from decimal import Decimal
import json
import urllib

print('Loading function')

rekognition = boto3.client('rekognition')


# --------------- Helper Functions to call Rekognition APIs ------------------


def detect_moderation_labels(bucket, key, inputParams):
    returnResult = {
        'Stage' : 'DetectModerationLabels',
        'Pass': True,
        'ErrorMessages': []
    }
    response = rekognition.detect_moderation_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": key}}, MinConfidence=10)
    for modLabel in response['ModerationLabels']:
        if((modLabel['ParentName'] == 'Explicit Nudity' or modLabel['Name'] == 'Explicit Nudity') and Decimal(str(modLabel['Confidence'])) >= 70):
            # print('Image has Explicit Content.')
            returnResult['Pass'] = False
            returnResult['ErrorMessages'].append('Image has Explicit Content.')
            break
        if((modLabel['ParentName'] == 'Suggestive' or modLabel['Name'] == 'Suggestive') and Decimal(str(modLabel['Confidence'])) >= 70):
            # print('Image has Suggestive Content.')
            returnResult['Pass'] = False
            returnResult['ErrorMessages'].append('Image has Suggestive Content.')
            break

    # process overall result
    inputParams['OverallResult']['Details'].append(returnResult)
    inputParams['OverallResult']['Pass'] = inputParams['OverallResult']['Pass'] and returnResult['Pass'] 
    if (returnResult['Pass'] is False):
        inputParams['OverallResult']['Reason'] = 'IMAGE_MODERATION_APPLIED'
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
        # Calls rekognition DetectModerationLabels API to detect faces in S3 object
        response = detect_moderation_labels(bucket, key, event)

        # Print response to console.
        # print(response)

        return response
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
