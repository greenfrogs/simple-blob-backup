from dotenv import load_dotenv

load_dotenv()

import os, tarfile, sys
from datetime import datetime
from azure.storage.blob import BlobServiceClient, StandardBlobTier

import socket
from urllib import request, parse

connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
container = os.getenv('CONTAINER')
directory = os.getenv('DIRECTORY')
ping_url = os.getenv('PING_URL')

try:
    request.urlopen(ping_url + '/start', timeout=10)
except socket.error as e:
    print("Ping failed: %s" % e)

if len(sys.argv) == 1:
    type = 'daily'
else:
    type = sys.argv[1]

count = int(os.getenv(type.upper()))

print('Connecting to container')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
name = type + '-' + datetime.now().strftime('%Y-%m-%d--%H-%M-%S') + '.tar.bz2'

try:
    blob_service_client.create_container(container)
except Exception as e:
    pass

print('Creating backup %s' % name)
with tarfile.open(name, "w:bz2") as tar:
    tar.add(directory, arcname=os.path.basename(directory))

container_client = blob_service_client.get_blob_client(container=container, blob=name)
tier = StandardBlobTier[os.getenv(type.upper() + '_TYPE').capitalize()]

print('Uploading blob as %s' % str(tier))
with open(name, "rb") as data:
    container_client.upload_blob(data, standard_blob_tier=tier)

os.remove(name)

container_client=blob_service_client.get_container_client(container)
blob_list = container_client.list_blobs(name_starts_with=(type + '-'))
all_blobs = []
for blob in blob_list:
    all_blobs.append((blob.last_modified, blob))

all_blobs.sort(key=lambda x: x[0], reverse=True)
while len(all_blobs) > count:
    blob = all_blobs.pop()
    print('Deleting %s' % blob[1].name)
    container_client.delete_blob(blob[1])

try:
    data = parse.urlencode({'type': type, 'name': name}).encode()
    request.urlopen(ping_url, data=data, timeout=10)
except socket.error as e:
    print("Ping failed: %s" % e)