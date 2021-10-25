import boto3
from botocore.client import Config
import requests
import json
from requests.auth import HTTPBasicAuth

file = 'session.csv'

# Upload to S3
s3 = boto3.resource('s3',
                    endpoint_url='https://localhost',
                    aws_access_key_id='dev',
                    aws_secret_access_key='12345678',
                    config=Config(signature_version='s3v4'),
                    verify=False)


s3.Bucket('ephemere').upload_file(file,file)

# Create Command
url = "http://127.0.0.1:3000/commande"
insert = {'utilisateur':'yop0382', 'storage_path': file}
headers = {'Prefer': 'return=representation'}
x = requests.post(url, data = insert, headers=headers)
print(x.text)

r = json.loads(x.text)
commande_id = r[0]['command_id']
print(commande_id)

# Launch psql function to send rabbitmq messages
url = "http://localhost:15672/api/exchanges/%2F/commands/publish"
headers = {'Content-Type': 'text/plain'}
message = json.JSONEncoder().encode(r[0])
print(message)
insert = """{"vhost": "/", "name": "commands", "properties": {"delivery_mode": 1, "headers": {}}, "routing_key": "commands", "delivery_mode": "1", "payload": '""" + message + """', "headers": {}, "props": {}, "payload_encoding": "string"}"""
print(insert)
x = requests.post(url, data=insert, auth=HTTPBasicAuth('admin', '1234'), headers=headers)
print(x.text)

