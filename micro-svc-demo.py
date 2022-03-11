#!/usr/bin/env -S python3 -W ignore

'''
Python Microservice
'''

import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import daemon
import signal
import json
from functools import partial
from couchbase_core._libcouchbase import LOCKMODE_NONE
from couchbase.cluster import Cluster, QueryOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import DocumentNotFoundException
from datetime import timedelta

PORT_NUMBER = 8080
LOG_FILE = '/tmp/service.out'

class dbConnection(object):

    def __init__(self, cluster, bucket, scope, collection, bucket_name, scope_name, collection_name):
        self.cluster_object = cluster
        self.bucket_object = bucket
        self.scope_object = scope
        self.collection_object = collection
        self.bucket_name = bucket_name
        self.scope_name = scope_name
        self.collection_name = collection_name


class serviceDefinition(object):

    def __init__(self, srv_host, srv_port, cb_host, cb_user, cb_pass, bucket, ssl=False):
        self.server_hostname = srv_host
        self.server_port = srv_port
        self.couchbase_host = cb_host
        self.couchbase_user = cb_user
        self.couchbase_password = cb_pass
        self.couchbase_bucket = bucket
        self.use_ssl = ssl


class couchbaseDriver(object):

    def __init__(self, hostname, username, password, ssl=False, internal=False):
        self.hostname = hostname
        self.auth = PasswordAuthenticator(username, password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=30),
                                              kv_timeout=timedelta(seconds=30))
        if internal:
            net_string = 'default'
        else:
            net_string = 'external'
        if ssl:
            self.cbcon = "couchbases://"
            self.opts = "?ssl=no_verify&config_total_timeout=15&config_node_timeout=10&network=" + net_string
        else:
            self.cbcon = "couchbase://"
            self.opts = "?config_total_timeout=15&config_node_timeout=10&network=" + net_string

    def connect(self, bucket_name, scope_name, collection_name):
        try:
            cluster_object = Cluster(self.cb_string,
                                   authenticator=self.auth,
                                   lockmode=LOCKMODE_NONE,
                                   timeout_options=self.timeouts)
            bucket_object = cluster_object.bucket(bucket_name)
            scope_object = bucket_object.scope(scope_name)
            collection_object = scope_object.collection(collection_name)
            self.dbObject = dbConnection(cluster_object,
                                         bucket_object,
                                         scope_object,
                                         collection_object,
                                         bucket_name,
                                         scope_name,
                                         collection_name)
            return True
        except Exception as e:
            print("Can not connect to host {} bucket {} collection {}: {}".format(
                self.hostname,
                bucket_name,
                collection_name,
                e))
            raise

    def get(self, key):
        if self.dbObject:
            try:
                key_name = self.formatKey(key)
                result = self.dbObject.collection_object.get(key_name)
                return result.content_as[dict]
            except DocumentNotFoundException:
                return None
            except Exception as e:
                print("Can not get key {} from collection {}: {}".format(
                    key,
                    self.dbObject.collection_name,
                    e))
                raise

    def query(self, field, key, value):
        if self.dbObject:
            contents = []
            keyspace = self.dbObject.bucket_name + '.' + self.dbObject.scope_name + '.' + self.dbObject.collection_name
            query = "SELECT " + field + " FROM " + keyspace + " WHERE " + key + " = \"" + value + "\";"
            try:
                result = self.dbObject.cluster_object.query(query, QueryOptions(metrics=False, adhoc=False))
                for item in result:
                    contents.append(item)
                return contents
            except Exception as e:
                print("Can not get key {} from collection {}: {}".format(
                    key,
                    self.dbObject.collection_name,
                    e))
                raise

    def formatKey(self, key):
        return self.dbObject.collection_name + ':' + key

    @property
    def cb_string(self):
        return self.cbcon + self.hostname + self.opts


class restServer(BaseHTTPRequestHandler):

    def __init__(self, db, *args, **kwargs):
        self.db = db
        super().__init__(*args, **kwargs)

    def bad_request(self):
        self.send_response(400)

    def not_found(self):
        self.send_response(404)

    def forbidden(self):
        self.send_response(403)

    def server_error(self):
        self.send_response(500)

    def v1_get_records(self, key, value):
        records = []
        result = self.db.query('record_id', key, value)
        if len(result) > 0:
            for item in result:
                record_data = self.db.get(item['record_id'])
                records.append(record_data)
        return records

    def v1_get_by_id(self, id):
        records = []
        result = self.db.get(id)
        if result:
            records.append(result)
        return records

    def v1_responder(self, records):
        if len(records) > 0:
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(bytes(json.dumps(records), "utf-8"))
        else:
            self.not_found()

    def do_GET(self):
        try:
            request_root = self.path.split('/')[1]
            request_version = self.path.split('/')[2]
            request_method = self.path.split('/')[3]
            request_parameter = self.path.split('/')[4]
        except IndexError:
            self.bad_request()
            return

        if request_root != 'api' or request_version != 'v1':
            self.forbidden()
            return

        try:
            if request_method == 'nickname':
                records = self.v1_get_records('nickname', request_parameter)
            elif request_method == 'username':
                records = self.v1_get_records('user_id', request_parameter)
            elif request_method == 'id':
                records = self.v1_get_by_id(request_parameter)
            else:
                self.forbidden()
                return
            self.v1_responder(records)
        except Exception as e:
            print("Server error: {}".format(e))
            self.server_error()
            return


class microService(object):

    def __init__(self, host, port, db):
        self.hostname = host
        self.port = port
        restHandler = partial(restServer, db)
        self.server = HTTPServer((self.hostname, self.port), restHandler)

    def start(self):
        self.server.serve_forever()

    def stop(self):
        self.server.server_close()


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-d', '--daemon', action='store_true')
    parser.add_argument('-h', '--host', action='store', default='')
    parser.add_argument('-p', '--port', action='store', default=PORT_NUMBER)
    parser.add_argument('-l', '--log', action='store', default=LOG_FILE)
    parser.add_argument('-c', '--cluster', action='store', required=True)
    parser.add_argument('-b', '--bucket', action='store', required=True)
    parser.add_argument('-u', '--user', action='store', required=True)
    parser.add_argument('-P', '--password', action='store', required=True)
    parser.add_argument('--tls', action='store_true')
    parser.add_argument('--debug', action='store', default=3)
    parser.add_argument('-?', action='help')
    args = parser.parse_args()
    return args

def main():
    args = parse_args()

    couchbase_server = couchbaseDriver(args.cluster, args.user, args.password, ssl=args.tls)
    result = couchbase_server.connect(args.bucket, 'profiles', 'user_data')

    if not result:
        print("Can not connect to database.")
        sys.exit(1)

    server = microService(args.host, args.port, couchbase_server)
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
