import os

import requests

import json

headers = {
    'cache-control': 'no-cache',
    # Already added when you pass json=
    # 'content-type': 'application/json',
}

json_data = {
    'username': 'admin',
    'password': '1d8najVt4zUA',
}

responsetkn = requests.post('https://cpd-cp4d.itzroks-270006dwv1-geb8q6-6ccd7f378ae819553d37d5f2ee142bd6-0000.us-south.containers.appdomain.cloud/icp4d-api/v1/authorize', headers=headers, json=json_data, verify=False)

#person_dict = json.loads(responsetkn)
opdata = responsetkn.json()
atoken = opdata['token']
print(responsetkn)
print("**************")

#atoken = person_dict['token']

print(atoken)

headers1 = {
    'Authorization': f"Bearer {atoken}",
    'Content-Type': 'application/json',
}

with open('/Users/mlakshm/plkafkajars.json') as f:
    data = f.read().replace('\n', '')

response = requests.post('https://cpd-cp4d.itzroks-270006dwv1-geb8q6-6ccd7f378ae819553d37d5f2ee142bd6-0000.us-south.containers.appdomain.cloud/v2/spark/v3/instances/3449514d-1ff1-4927-9979-d8593902160b/spark/applications', headers=headers1, data=data, verify=False)

print("*******************")
print(response)
