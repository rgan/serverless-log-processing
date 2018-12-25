import json

import requests
import os


def handler(event, context):
    batch_id = event.get('batch_id')
    emrHost = os.environ["emrMaster"]
    s3_base_path = os.environ["s3_base_path"]
    input_path = "{0}/input/{1}".format(s3_base_path, batch_id)
    scores_path = "{0}/scores/{1}".format(s3_base_path, batch_id)
    output_path = "{0}/output/{1}".format(s3_base_path, batch_id)
    jar_path = os.environ["jar_path"]
    class_name = os.environ["class_name"]
    url = 'http://{0}:8998/batches'.format(emrHost)
    headers = { "content-type": "application/json" }
    payload = {"file" : "{0}/{1}".format(s3_base_path, jar_path),
               "className" : class_name, "args" : [input_path, scores_path, output_path]}
    response = requests.post(url, data=json.dumps(payload), headers=headers, verify=False)
    return json.loads(response.text).get('id')


