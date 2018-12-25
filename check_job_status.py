import requests
import os


def handler(event, context):
    jobid = event.get('jobId')
    emrHost = os.environ["emrMaster"]
    url = 'http://{0}:8998/batches/{1}'.format(emrHost, jobid)
    data = requests.get(url).json()
    return data.get('state')