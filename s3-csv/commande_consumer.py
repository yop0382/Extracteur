#!/usr/bin/env python3
import urllib.parse
import boto3
from botocore.client import Config
import pika
import json
import tempfile
import requests
import os
import time
import csv
import requests
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed

class Consumer:
    s3 = None
    channel = None

    def main(self):
        # S3 configuration
        self.s3 = boto3.resource('s3',
                            endpoint_url='https://localhost',
                            aws_access_key_id='dev',
                            aws_secret_access_key='12345678',
                            config=Config(signature_version='s3v4'),
                            verify=False)
        
        # Rabbitmq configuration
        connection = pika.BlockingConnection(pika.URLParameters('amqp://admin:1234@127.0.0.1:5672/%2F'))
        channel = connection.channel()
        self.channel = channel
        channel.basic_qos(prefetch_count=100)
        channel.basic_consume('commands', self.on_message)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
        connection.close()

    def request_post(self, url, data, auth, headers):
        insert = """{"vhost":"/","name":"amq.direct","properties":{"delivery_mode":1,"headers":{}},"routing_key":"events","delivery_mode":"1","payload": '""" + str(data) + """',"headers":{},"props":{},"payload_encoding":"string"}"""
        #return requests.post(url, data=insert, auth=auth, headers=headers)
        return self.channel.basic_publish(exchange='amq.direct',routing_key='events',body=insert)

    def on_message(self, channel, method_frame, header_frame, body):
        print(body.decode('utf-8'))
        jbody = json.loads(body.decode('utf-8'))
        storage_path = jbody['storage_path']
        command_id = jbody['command_id']
        print(jbody)

        print(self.s3)
        start = time.time()

        # Dowload S3 file
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        tmp_name = tmp.name
        self.s3.Bucket('ephemere').download_file(storage_path, tmp_name)

        # Add column command_id
        tmp2 = tempfile.NamedTemporaryFile(delete=False)
        tmp2.close()
        tmp2_name = tmp2.name
        cData = []
        with open(tmp_name) as csvFile:
            csvReader = csv.DictReader(csvFile)
            to_add = ['session_id', 'command_id']
            cData.append(to_add)
            for i, row in enumerate(csvReader):
              to_add = [row['session_id'], command_id]
              cData.append(to_add)

        with open(tmp2_name, 'w', newline='') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerows(cData)


        # Bulk insert with postgrest
        url = "http://127.0.0.1:3000/event?columns=session_id,command_id"
        headers = {'Content-Type': 'text/csv'}
        with open(tmp2_name, 'rb') as f:
          x = requests.post(url, headers=headers, data=f)

        # Send rabbitmq event
        url = "http://localhost:15672/api/exchanges/%2F/amq.direct/publish"
        headers = {'Content-Type': 'text/plain'}
        auth = HTTPBasicAuth('admin', '1234')

        self.channel.basic_qos(prefetch_count=5 * 10)

        processes = []
        for data in cData:
            data = json.JSONEncoder().encode(data)
            insert = """{"vhost":"/","name":"amq.direct","properties":{"delivery_mode":1,"headers":{}},"routing_key":"events","delivery_mode":"1","payload": '""" + str(data) + """',"headers":{},"props":{},"payload_encoding":"string"}"""
            self.channel.basic_publish(exchange='amq.direct', routing_key='events', body=insert)

       # with ThreadPoolExecutor(max_workers=2) as executor:
       #   for data in cData:
       #     data = json.JSONEncoder().encode(data)
       #     self.channel.basic_publish(exchange='amq.direct', routing_key='events', body=insert)
            # insert = """{"vhost":"/","name":"amq.direct","properties":{"delivery_mode":1,"headers":{}},"routing_key":"events","delivery_mode":"1","payload": '""" + str(data) + """',"headers":{},"props":{},"payload_encoding":"string"}"""
            #print(insert)
            # processes.append(executor.submit(self.request_post, url, data, auth, headers))

        #print(processes)
        #with concurrent.futures.ThreadPoolExecutor() as executor:  # optimally defined number of threads
        #    res = [executor.submit(self.request_post, url, data=data, auth=auth, headers=headers) for data in cData]
        #    concurrent.futures.wait(res)

        #for event in cData:
        #  headers = {'Content-Type': 'text/plain'}
        #  insert = """{"vhost":"/","name":"amq.direct","properties":{"delivery_mode":1,"headers":{}},"routing_key":"events","delivery_mode":"1","payload":" """ + str(event) + """ ","headers":{},"props":{},"payload_encoding":"string"}"""
        #  x = requests.post(url, data=insert, auth=HTTPBasicAuth('admin', '1234'), headers=headers)
        #  print(x.text)

        end = time.time()
        print(end - start)

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)


if __name__ == '__main__':
    Consumer().main()