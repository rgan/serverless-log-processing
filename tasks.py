import json
import tempfile

import boto3
from invoke import task

S3_BUCKET = "rg-log-processing"

# packages "checkJobStatus.zip" and "submitJob.zip"
@task
def deploy_lambda_functions_to_s3(context):
    context.run("rm -rf build && mkdir build && cd build && virtualenv -ppython3 env "
        "&& source env/bin/activate && pip install -r ../requirements.txt && "
        "cd env/lib/python3.6/site-packages && zip -r9 ../../../../dependencies.zip .")
    context.run("cd build && cp dependencies.zip checkJobStatus.zip && "
                "zip -g checkJobStatus.zip ../check_job_status.py")
    context.run("cd build && cp dependencies.zip submitJob.zip && "
                "zip -g submitJob.zip ../submit_spark_job.py")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('build/checkJobStatus.zip', S3_BUCKET, 'checkJobStatus.zip')
    s3.meta.client.upload_file('build/submitJob.zip', S3_BUCKET, 'submitJob.zip')


@task
def deploy_spark_jar_to_s3(context):
    context.run("sbt package")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('target/scala-2.11/spark-project_2.11-1.0.jar',
                               S3_BUCKET, 'spark_job.jar')

@task
def setup_test_data(context, batch_id):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file("tests/app_logs.json", S3_BUCKET, "input/{0}/app_log.json".format(batch_id))
    s3.meta.client.upload_file("tests/scores.json", S3_BUCKET, "scores/{0}/scores.json".format(batch_id))