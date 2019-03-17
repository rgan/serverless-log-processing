import json
from time import sleep

import boto3
from botocore.exceptions import ClientError
from invoke import task

STACK_NAME = "etl-state-machine"
CODE_BUCKET_STACK_NAME = "etl-state-machine-code-bucket"

# packages "checkJobStatus.zip" and "submitJob.zip"
@task
def deploy_lambda_functions_to_s3(context, bucket):
    context.run("rm -rf build && mkdir build && cd build && virtualenv -ppython3 env "
        "&& source env/bin/activate && pip install -r ../requirements.txt && "
        "cd env/lib/python3.6/site-packages && zip -r9 ../../../../dependencies.zip .")
    context.run("cd build && cp dependencies.zip checkJobStatus.zip && "
                "zip -g checkJobStatus.zip ../check_job_status.py")
    context.run("cd build && cp dependencies.zip submitJob.zip && "
                "zip -g submitJob.zip ../submit_spark_job.py")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('build/checkJobStatus.zip', bucket, 'checkJobStatus.zip')
    s3.meta.client.upload_file('build/submitJob.zip', bucket, 'submitJob.zip')


@task
def deploy_spark_jar_to_s3(context, bucket):
    context.run("sbt package")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('target/scala-2.11/spark-project_2.11-1.0.jar',
                               bucket, 'spark_job.jar')


def wait_for_stack_status(client, stack_name, starting_status, final_status):
    status = starting_status
    outputs = None
    while status == starting_status:
        sleep(30)
        print("Waiting for {0} ...".format(final_status))
        try:
            response = client.describe_stacks(StackName=stack_name)
            this_stack = response['Stacks'][0]
            status = this_stack['StackStatus']
            if "Outputs" in this_stack:
                outputs = this_stack['Outputs']
        except ClientError:
            status = final_status
    if status != final_status:
        raise Exception("Stack {0} creation failed: {1}".format(stack_name, status))


def name_with_env(name, env):
    return "{0}-{1}".format(name, env)


def full_stack_name(env):
    return name_with_env(STACK_NAME, env)


def setup_full_stack(context, env):
    config = json.loads(open("conf/{0}/config.json".format(env)).read())
    client = boto3.client('cloudformation')
    stack_name = full_stack_name(env)
    tags = [
        { "Key" : "project",
          "Value" : "etl"
        }
    ]
    client.create_stack(
        StackName = stack_name,
        TemplateBody = open("etl_state_machine_cf.json").read(),
        Parameters = [
            {
                'ParameterKey' : 'SubnetID',
                'ParameterValue' : config['subnet_id']
            },
            {
                'ParameterKey': 'KeyName',
                'ParameterValue': config['key_pair_name']
            },
            {
                'ParameterKey': 'VPCID',
                'ParameterValue': config['vpc_id']
            },
            {
                'ParameterKey': 'S3BucketName',
                'ParameterValue': config['bucket']
            },
            {
                'ParameterKey': 'S3CodeBucketName',
                'ParameterValue': config['code_bucket']
            }
        ],
        Capabilities=[ 'CAPABILITY_NAMED_IAM'],
        Tags = tags
    )
    wait_for_stack_status(client, stack_name, "CREATE_IN_PROGRESS", "CREATE_COMPLETE")


def get_bucket(bucket_name):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404' or error_code == '403' :
            return None
    return bucket


def delete_keys_and_bucket(bucket_name):
    bucket = get_bucket(bucket_name)
    if bucket:
        for obj in bucket.objects.all():
            obj.delete()
        bucket.delete()


def stack_exists(cloud_formation, stack_name):
    try:
        cloud_formation.describe_stacks(StackName=stack_name)
    except ClientError:
        return False
    return True


def delete_stack_if_exists(name):
    cloud_formation = boto3.client("cloudformation")
    if stack_exists(cloud_formation, name):
        cloud_formation.delete_stack(StackName=name)
        wait_for_stack_status(cloud_formation, name, 'DELETE_COMPLETE', 'DELETE_IN_PROGRESS')


@task
def delete_all(context, env="dev"):
    config = json.loads(open("conf/{0}/config.json".format(env)).read())
    delete_keys_and_bucket(config["code_bucket"])
    delete_keys_and_bucket(config["bucket"])
    delete_stack_if_exists(name_with_env(CODE_BUCKET_STACK_NAME, env))
    delete_stack_if_exists(full_stack_name(env))


@task
def setup_code_bucket(context, env="dev"):
    config = json.loads(open("conf/{0}/config.json".format(env)).read())
    client = boto3.client('cloudformation')
    stack_name = name_with_env(CODE_BUCKET_STACK_NAME, env)
    tags = [
        {"Key": "project",
         "Value": "etl"
         }
    ]
    client.create_stack(
        StackName=stack_name,
        TemplateBody=open("code_bucket_template.json").read(),
        Parameters=[
            {
                'ParameterKey': 'CodeBucketName',
                'ParameterValue': config['code_bucket']
            }
        ],
        Tags=tags
    )
    wait_for_stack_status(client, stack_name, "CREATE_IN_PROGRESS", "CREATE_COMPLETE")


@task
def deploy_all(context, env="dev"):
    config = json.loads(open("conf/{0}/config.json".format(env)).read())
    setup_code_bucket(context)
    code_bucket = config["code_bucket"]
    deploy_lambda_functions_to_s3(context, code_bucket)
    deploy_spark_jar_to_s3(context, code_bucket)
    setup_full_stack(context, env)