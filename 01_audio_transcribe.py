"""
Author: Danny Witt
Purpose: read s3 bucket with audio files, apply transcribe function, and 
    deposit resulting .json output file in intermediate s3 bucket
Input: s3 bucket path with source audio files
Output: s3 bucket path to deposit output .json transcribe (medical) files
"""
from __future__ import print_function
import time
import json
import boto3
import botocore

######################################
#LOADING AUDIO .MP4 FILES FROM S3
######################################

s3 = boto3.resource('s3')
bucket_in = s3.Bucket('input-audio-rwitt-research')
bucket_out_nm = 'intermediate-transcribe-rwitt-research'
transcribe = boto3.client('transcribe')

for obj in bucket_in.objects.all():
    bucket_nm = obj.bucket_name
    obj_key = obj.key
    file_uri = 's3://{}/{}'.format(bucket_nm, obj_key)
    job_name = obj_key.split('_audio_only')[0].replace(".", "_") + '_transcribed'
    
    print('Processing file: {}'.format(job_name))
    
    ######################################
    #APPLY TRANSCRIBE MEDICAL TO AUDIO FILES
    ######################################
    transcribe.start_medical_transcription_job(
        MedicalTranscriptionJobName= job_name,
        LanguageCode='en-US',
        MediaFormat='mp4',
        Media={
            'MediaFileUri': file_uri
        },
        OutputBucketName= bucket_out_nm,
        Settings= {
            "MaxSpeakerLabels": 2,
            "ShowSpeakerLabels": True
        },
        Specialty='PRIMARYCARE',
        Type='CONVERSATION'
    )
    
    while True:
        status = transcribe.get_medical_transcription_job(MedicalTranscriptionJobName=job_name)
        if status['MedicalTranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Not ready yet...")
        time.sleep(100)
    print(status)