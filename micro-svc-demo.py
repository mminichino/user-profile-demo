#!/usr/bin/env -S python3 -W ignore -u

'''
Python Microservice
'''

import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import daemon
import signal
import json
import base64
from distutils.util import strtobool
from functools import partial
from couchbase_core._libcouchbase import LOCKMODE_NONE
from couchbase.cluster import Cluster, QueryOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import DocumentNotFoundException
import couchbase.subdocument as SD
from datetime import timedelta

PORT_NUMBER = 8080
LOG_FILE = '/tmp/service.out'

class dbConnection(object):

    def __init__(self, cluster, bucket, scope, collections):
        self.cluster = cluster
        self.bucket = bucket
        self.scope = scope
        self.collections = collections


class couchbaseDriver(object):

    def __init__(self, hostname, username, password, ssl=False, external=False):
        self.hostname = hostname
        self.auth = PasswordAuthenticator(username, password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=30),
                                              kv_timeout=timedelta(seconds=30))
        if external:
            net_string = 'external'
        else:
            net_string = 'default'
        if ssl:
            self.cbcon = "couchbases://"
            self.opts = "?ssl=no_verify&config_total_timeout=15&config_node_timeout=10&network=" + net_string
        else:
            self.cbcon = "couchbase://"
            self.opts = "?config_total_timeout=15&config_node_timeout=10&network=" + net_string

    def connect(self, bucket_name, scope_name, *collections):
        bucket_dict = {}
        scope_dict = {}
        collection_dict = {}
        try:
            cluster_object = Cluster(self.cb_string,
                                   authenticator=self.auth,
                                   lockmode=LOCKMODE_NONE,
                                   timeout_options=self.timeouts)
            bucket_object = cluster_object.bucket(bucket_name)
            bucket_dict[bucket_name] = bucket_object
            scope_object = bucket_object.scope(scope_name)
            scope_dict[scope_name] = scope_object
            for collection_name in collections:
                collection_object = scope_object.collection(collection_name)
                collection_dict[collection_name] = collection_object
            self.dbObject = dbConnection(cluster_object,
                                         bucket_dict,
                                         scope_dict,
                                         collection_dict)
            return True
        except Exception as e:
            print("Can not connect to host {} bucket {} collections {}: {}".format(
                self.hostname,
                bucket_name,
                ",".join(collections),
                e))
            raise

    def get(self, collection, key):
        if self.dbObject:
            try:
                key_name = self.formatKey(collection, key)
                result = self.dbObject.collections[collection].get(key_name)
                return result.content_as[dict]
            except DocumentNotFoundException:
                return None
            except Exception as e:
                print("Can not get key {} from collection {}: {}".format(
                    key,
                    collection,
                    e))
                raise

    def get_value(self, collection, key, field):
        if self.dbObject:
            try:
                key_name = self.formatKey(collection, key)
                result = self.dbObject.collections[collection].lookup_in(key_name, [SD.get(field)])
                return result.content_as[str](0)
            except DocumentNotFoundException:
                return None
            except Exception as e:
                print("Can not get key {} from collection {}: {}".format(
                    key,
                    collection,
                    e))
                raise

    def query(self, collection, field, key, value):
        bucket_name = list(self.dbObject.bucket.keys())[0]
        scope_name = list(self.dbObject.scope.keys())[0]
        if self.dbObject:
            contents = []
            keyspace = bucket_name + '.' + scope_name + '.' + collection
            query = "SELECT " + field + " FROM " + keyspace + " WHERE " + key + " = \"" + value + "\";"
            try:
                result = self.dbObject.cluster.query(query, QueryOptions(metrics=False, adhoc=False))
                for item in result:
                    contents.append(item)
                return contents
            except Exception as e:
                print("Can not get key {} from collection {}: {}".format(
                    key,
                    collection,
                    e))
                raise

    def formatKey(self, collection, key):
        return collection + ':' + key

    @property
    def cb_string(self):
        return self.cbcon + self.hostname + self.opts


class restServer(BaseHTTPRequestHandler):
    TYPE_JSON = 0
    TYPE_IMAGE = 1

    def __init__(self, db, token, *args, **kwargs):
        self.db = db
        self.auth_token = token
        super().__init__(*args, **kwargs)

    def bad_request(self):
        self.send_response(400)

    def unauthorized(self):
        self.send_response(401)

    def not_found(self):
        self.send_response(404)

    def forbidden(self):
        self.send_response(403)

    def server_error(self):
        self.send_response(500)

    def get_image_data(self, records):
        codec = None
        image = None
        try:
            codec = records[0]['type']
            image = records[0]['image']
        except (KeyError, IndexError, TypeError):
            pass
        return image, codec

    def v1_get_records(self, collection, key, value):
        result = self.db.query(collection, '*', key, value)
        return result

    def v1_get_by_id(self, collection, key):
        records = []
        result = self.db.get(collection, key)
        if result:
            records.append(result)
        return records

    def v1_responder(self, records, response_type):
        content_length = None
        if len(records) > 0:
            if response_type == restServer.TYPE_JSON:
                content_type = "application/json"
                response_body = bytes(json.dumps(records), "utf-8")
            else:
                image, codec = self.get_image_data(records)
                if not image or not codec:
                    self.bad_request()
                    return
                content_type = "image/{}".format(codec)
                response_body = base64.b64decode(bytes(image, "utf-8"))
                content_length = len(response_body)
            self.send_response(200)
            self.send_header("Content-type", content_type)
            if content_length:
                self.send_header("Content-Length", str(content_length))
            self.end_headers()
            self.wfile.write(response_body)
        else:
            self.not_found()

    def v1_responder_image(self, image, codec):
        type_string = "image/{}".format(codec)
        self.send_response(200)
        self.send_header("Content-type", type_string)
        self.end_headers()
        self.wfile.write(image)

    def v1_check_auth_token(self, headers):
        for key in headers:
            if key == 'authorization' or key == 'x-access-token':
                token = headers[key]
                if token.startswith('Bearer '):
                    token = token[len('Bearer '):]
                if token == self.auth_token:
                    return True
        return False

    def do_GET(self):
        request_qualifier = None
        response_type = restServer.TYPE_JSON
        headers = {k.lower(): v for k, v in self.headers.items()}
        if not self.v1_check_auth_token(headers):
            self.unauthorized()
            return
        try:
            get_elements = self.path.split('/')
            request_root = get_elements[1]
            request_version = get_elements[2]
            request_method = get_elements[3]
            request_parameter = get_elements[4]
            if len(get_elements) > 5:
                request_qualifier = request_parameter
                request_parameter = get_elements[5]
        except IndexError:
            self.bad_request()
            return

        if request_root != 'api' or request_version != 'v1':
            self.forbidden()
            return

        try:
            if request_method == 'nickname':
                records = self.v1_get_records('user_data', 'nickname', request_parameter)
            elif request_method == 'username':
                records = self.v1_get_records('user_data', 'user_id', request_parameter)
            elif request_method == 'id':
                records = self.v1_get_by_id('user_data', request_parameter)
            elif request_method == 'picture':
                if not request_qualifier:
                    self.bad_request()
                if request_qualifier == 'record':
                    pass
                elif request_qualifier == 'raw':
                    response_type = restServer.TYPE_IMAGE
                else:
                    self.forbidden()
                    return
                records = self.v1_get_by_id('user_images', request_parameter)
            else:
                self.forbidden()
                return
            self.v1_responder(records, response_type)
        except Exception as e:
            print("Server error: {}".format(e))
            self.server_error()
            return


class microService(object):

    def __init__(self, host, port, db, token):
        self.hostname = host
        self.port = port
        restHandler = partial(restServer, db, token)
        self.server = HTTPServer((self.hostname, self.port), restHandler)

    def start(self):
        self.server.serve_forever()

    def stop(self):
        self.server.server_close()


def parse_args():
    host_default = os.environ['COUCHBASE_HOST'] if os.environ.get('COUCHBASE_HOST') else "localhost"
    user_default = os.environ['COUCHBASE_USER'] if os.environ.get('COUCHBASE_USER') else "Administrator"
    pass_default = os.environ['COUCHBASE_PASSWORD'] if os.environ.get('COUCHBASE_PASSWORD') else "password"
    bucket_default = os.environ['COUCHBASE_BUCKET'] if os.environ.get('COUCHBASE_BUCKET') else "sample_app"
    net_default = os.environ['COUCHBASE_NETWORK'] if os.environ.get('COUCHBASE_NETWORK') else "False"
    ssl_default = os.environ['COUCHBASE_TLS'] if os.environ.get('COUCHBASE_TLS') else "True"
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-d', '--daemon', action='store_true')
    parser.add_argument('-h', '--host', action='store', default='')
    parser.add_argument('-p', '--port', action='store', default=PORT_NUMBER)
    parser.add_argument('-l', '--log', action='store', default=LOG_FILE)
    parser.add_argument('-c', '--cluster', action='store', default=host_default)
    parser.add_argument('-b', '--bucket', action='store', default=bucket_default)
    parser.add_argument('-u', '--user', action='store', default=user_default)
    parser.add_argument('-P', '--password', action='store', default=pass_default)
    parser.add_argument('-e', '--external', action='store', default=net_default)
    parser.add_argument('-S', '--tls', action='store', default=ssl_default)
    parser.add_argument('--debug', action='store', default=3)
    parser.add_argument('-?', action='help')
    args = parser.parse_args()
    return args

def get_auth_token(db):
    key_id = '1'
    response = db.get_value('service_auth', key_id, 'token')
    if (len(response)) > 0:
        return response
    else:
        raise Exception("Can not fetch auth token")

def main():
    args = parse_args()
    net_arg = bool(strtobool(args.external))
    tls_arg = bool(strtobool(args.tls))

    print("Starting Service: [Cluster] {} [User] {} [Bucket] {} SSL: {}".format(
        args.cluster, args.user, args.bucket, tls_arg))

    couchbase_server = couchbaseDriver(args.cluster, args.user, args.password, ssl=tls_arg, external=net_arg)
    result = couchbase_server.connect(args.bucket, 'profiles', 'user_data', 'user_images', 'service_auth')

    if not result:
        print("Can not connect to database.")
        sys.exit(1)

    auth_token = get_auth_token(couchbase_server)

    server = microService(args.host, args.port, couchbase_server, auth_token)
    logfile = open(args.log, 'w')

    def signalHandler(signum, frame):
        server.stop()
        sys.exit(0)

    if args.daemon:
        context = daemon.DaemonContext(stdout=logfile, stderr=logfile)
        context.files_preserve = [server.server.fileno()]
        context.signal_map = {
            signal.SIGTERM: signalHandler,
            signal.SIGINT: signalHandler,
        }
        with context:
            server.start()
    else:
        signal.signal(signal.SIGINT, signalHandler)
        server.start()


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
