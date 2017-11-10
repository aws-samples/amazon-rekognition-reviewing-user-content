from __future__ import print_function

import boto3
from decimal import Decimal
import json
import urllib

print('Loading function')

rekognition = boto3.client('rekognition')


# --------------- Helper Functions to call Rekognition APIs ------------------


def detect_faces(bucket, key, inputParams):
    DetectFacesResult = {
        'Stage': 'DetectFaces',
        'Pass': True,
        'ErrorMessages': []
    }
    response = rekognition.detect_faces(
        Image={"S3Object": {"Bucket": bucket, "Name": key}}, Attributes=['ALL'])
    # print(str(len(response['FaceDetails'])))
    if (len(response['FaceDetails']) == 0):
        # print ('No Face Detected.')
        DetectFacesResult['Pass'] = False
        DetectFacesResult['ErrorMessages'].append('No Face Detected.')
    elif (len(response['FaceDetails']) > 1):
        # print ('More than one face detected.')
        DetectFacesResult['Pass'] = False
        DetectFacesResult['ErrorMessages'].append(
            'More than one face detected.')
    else:
        faceDetail = response['FaceDetails'][0]
        if (faceDetail.get('MouthOpen') is not None and faceDetail['MouthOpen']['Value'] is True and Decimal(str(faceDetail['MouthOpen']['Confidence'])) >= 90.0):
            # print('Face Detected with Mouth Open.')
            DetectFacesResult['Pass'] = False
            DetectFacesResult['ErrorMessages'].append(
                'Face Detected with Mouth Open.')
        if (faceDetail.get('Sunglasses') is not None and faceDetail['Sunglasses']['Value'] is True and Decimal(str(faceDetail['Sunglasses']['Confidence'])) >= 90.0):
            # print('Face Detected with Sunglasses.')
            DetectFacesResult['Pass'] = False
            DetectFacesResult['ErrorMessages'].append(
                'Face Detected with Sunglasses.')
        if (faceDetail.get('EyesOpen') is not None and faceDetail['EyesOpen']['Value'] is False and Decimal(str(faceDetail['EyesOpen']['Confidence'])) >= 90.0):
            # print('Face Detected with eyes closed.')
            DetectFacesResult['Pass'] = False
            DetectFacesResult['ErrorMessages'].append(
                'Face Detected with eyes closed.')
        if(faceDetail.get('Gender') is not None):
            # print ('Gender of Face = {}'.format(faceDetail['Gender']['Value']))
            DetectFacesResult['ErrorMessages'].append(
                'Gender of Face = {}'.format(faceDetail['Gender']['Value']))
        # Check for age range
        ageRange = faceDetail.get('AgeRange')
        if(ageRange is not None and int(str(ageRange['High'])) <= 18):
            # print ('Face corresponds to a minor')
            DetectFacesResult['Pass'] = False
            DetectFacesResult['ErrorMessages'].append('Face corresponds to a minor.')
        
        # Check for pose
        pose = faceDetail.get('Pose')
        if(pose is not None and (Decimal(str(pose['Pitch'])) <= -20 or Decimal(str(pose['Pitch'])) >= 20 or Decimal(str(pose['Roll'])) <= -20 or Decimal(str(pose['Roll'])) >= 20 or Decimal(str(pose['Yaw'])) <= -20 or Decimal(str(pose['Yaw'])) >= 20)):
            # print ('Face not looking in the right direction.')
            DetectFacesResult['Pass'] = False
            DetectFacesResult['ErrorMessages'].append(
                'Face not looking in the right direction.')

    

    # process overall result
    inputParams['OverallResult']['Details'].append(DetectFacesResult)
    inputParams['OverallResult']['Pass'] = inputParams['OverallResult']['Pass'] and DetectFacesResult['Pass'] 
    if (DetectFacesResult['Pass'] is False):
        inputParams['OverallResult']['Reason'] = 'FACIAL_ANALYSIS_FAILURE'
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
        # Calls rekognition DetectFaces API to detect faces in S3 object
        returnResult = detect_faces(bucket, key, event)

        # Print response to console.
        # print(response)

        return returnResult
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
