import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')
glue = boto3.client('glue')

GLUE_JOB_NAME = 'your-glue-job-name'  # 👈 Replace this with your actual Glue job name

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print(f"File uploaded: s3://{bucket}/{key}")
    
    try:
        # Optional: Validate it's the right path, like raw_data/
        if not key.startswith("raw_data/"):
            print("Not in /raw_data/, skipping...")
            return

        # Start Glue job
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--S3_INPUT_PATH': f's3://{bucket}/{key}'  # Optional: pass path if Glue script uses it
            }
        )
        print(f"Started Glue job: {response['JobRunId']}")
        return {
            'statusCode': 200,
            'body': f"Glue job {GLUE_JOB_NAME} started successfully."
        }

    except Exception as e:
        print(e)
        raise e
