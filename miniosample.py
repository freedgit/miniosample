#!/usr/bin/env python3

# the only requirement so far:
# pip install boto

# do not use the following in production code! This allows bypassing of ssl certificate check for self-signed code.
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# most of the code comes from https://docs.ceph.com/en/latest/radosgw/s3/python/
import boto
import boto.s3.connection
import os
import uuid

# I grab access_key, secret_key, port and host from here.
from credentials import *

from pdb import set_trace as stp

def gen_id():
    return str(uuid.uuid4())

conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = host,
        # is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        port = port
        )

print("existing buckets:")
for bucket in conn.get_all_buckets():
        print("{name}\t{created}".format(
                name = bucket.name,
                created = bucket.creation_date,
        ))
print("\n")


print("creating temporary bucket")
bucket_id = gen_id()
bucket = conn.create_bucket(bucket_id)

print("reading local files:")
for root, dirs, files in os.walk("."):
    for file_name in files:
        if file_name.startswith("2019"):
            key = bucket.new_key(gen_id() + ".jpg")
            key.set_contents_from_filename(file_name)

print("what files were created there?")
print("(need to delete everything before continuing otherwise I get a 409 conflict error)")
for key in bucket.list():
    print("{name}\t{size}\t{modified}".format(
            name = key.name,
            size = key.size,
            modified = key.last_modified,
    ))
    new_filename = "downloaded." + gen_id() + ".jpg"
    
    print("downloading to " + new_filename)
    sub_key = bucket.get_key(key.name)
    sub_key.get_contents_to_filename(new_filename)

    bucket.delete_key(key.name)

print("now deleting the bucket.")
conn.delete_bucket(bucket.name)
