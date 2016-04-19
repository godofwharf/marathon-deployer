import requests
import argparse
import json
import sys
import time
import traceback

import base64
import gitlab
import hashlib
import urllib
import threading
from Queue import LifoQueue
from argparse import Namespace
from bs4 import BeautifulSoup

import socket
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

PORT_NUMBER = 1729
event_queue = LifoQueue(1)


class eventHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        data = self.rfile.read(int(self.headers['Content-Length']))
        event = json.loads(data)
        print "Received event from marathon: {0}".format(event)
        if event["eventType"] in ["deployment_success", "deployment_failure"]:
            event_queue.put(event)
        return


def setup_httpserver():
    #Create a web server and define the handler to manage the
    #incoming request
    server = HTTPServer(('0.0.0.0', PORT_NUMBER), eventHandler)
    print 'Started httpserver on port ', PORT_NUMBER
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.setDaemon(True)
    server_thread.start()
    return server


def get_app(marathon_endpoint, app_id=None, label=None):
    get_response = None
    try:
        if app_id:
            get_response = requests.get(marathon_endpoint +
                                        "/v2/apps/{0}".format(app_id))
        else:
            if label:
                get_response = requests.get(marathon_endpoint +
                                            "/v2/apps?label={0}".format(
                                                urllib.quote_plus(label)))
            else:
                get_response = requests.get(marathon_endpoint + "/v2/apps")
    except requests.exceptions.RequestException as e:
        print "Get apps failed due to exception"
        raise
    get_response.raise_for_status()
    get_response_dict = {}
    if get_response.status_code == requests.codes.ok:
        get_response_dict = json.loads(get_response.text)
    else:
        print "Get apps failed appId={0}, labels={1}".format(app_id, labels)
        raise
    return get_response_dict


def get_callback_url():
    return "http://" + socket.gethostbyname('parent') + ":" + str(PORT_NUMBER)


def subscribe(marathon_endpoint, callback_url):
    try:
        subscribe_response = requests.post(
            marathon_endpoint +
            "/v2/eventSubscriptions?callbackUrl={0}".format(urllib.quote_plus(
                callback_url)),
            headers={'Content-Type': 'application/json'})
    except requests.exceptions.RequestException as e:
        print "Post event subscription failed due to exception"
        raise
    subscribe_response.raise_for_status()


def unsubscribe(marathon_endpoint, callback_url):
    try:
        unsubscribe_response = requests.delete(
            marathon_endpoint +
            "/v2/eventSubscriptions?callbackUrl={0}".format(urllib.quote_plus(
                callback_url)),
            headers={'Content-Type': 'application/json'})
    except requests.exceptions.RequestException as e:
        print "Delete event subscription failed due to exception"
        raise


def get_versions(marathon_endpoint, app_id):
    try:
        get_versions_response = requests.get(
            marathon_endpoint + "/v2/apps/{0}/versions".format(app_id))
    except requests.exceptions.RequestException as e:
        print "Get versions failed for appId={0} due to exception".format(
            app_id)
        raise
    get_versions_response.raise_for_status()
    get_versions_response_dict = json.loads(get_versions_response.text)
    return sorted([version for version in get_versions_response_dict[
        "versions"]])


def get_version(marathon_endpoint, app_id, version):
    try:
        get_version_response = requests.get(
            marathon_endpoint + "/v2/apps/{0}/versions/{1}".format(app_id,
                                                                   version))
    except requests.exceptions.RequestException as e:
        print "Get version failed for appId={0} and version={1} due to exception".format(
            app_id, version)
        raise
    get_version_response.raise_for_status()
    return json.loads(get_version_response.text)


def rollback(marathon_endpoint, deployment_id):
    try:
        rollback_response = requests.delete(
            marathon_endpoint + "/v2/deployments/{0}?force=false".format(
                deployment_id))
    except requests.exceptions.RequestException as e:
        print "Rollback failed for deploymentId={0} due to exception".format(
            deployment_id)
        raise
    rollback_response.raise_for_status()


def put_app(marathon_endpoint, app_id, config):
    try:
        subscribe(marathon_endpoint, get_callback_url())
        put_response = requests.put(
            marathon_endpoint + "/v2/apps/{0}".format(app_id),
            json.dumps(config,
                       ensure_ascii=False))
        put_response.raise_for_status()

        put_response_dict = json.loads(put_response.text)
        deployment_id = put_response_dict['deploymentId']
        event = event_queue.get()

        if event['eventType'] == 'deployment_failed' and event[
                'id'] == deployment_id:
            rollback(marathon_endpoint, deployment_id)
            return False
        print "Put app successful"
    except requests.exceptions.RequestException as e:
        print "Put app failed due to exception"
        raise
    finally:
        unsubscribe(marathon_endpoint, get_callback_url())
    return True


def post_app(marathon_endpoint, app_json):
    try:
        subscribe(marathon_endpoint, get_callback_url())
        app_json_dict = json.loads(app_json)
        app_id = app_json_dict['id']
        post_response = requests.post(
            marathon_endpoint + "/v2/apps",
            app_json,
            headers={'Content-Type': 'application/json'})
        post_response_dict = json.loads(post_response.text)
        deployment_id = post_response_dict['deployments'][0]['id']
        post_response.raise_for_status()

        post_response_dict = json.loads(post_response.text)
        event = event_queue.get()
        if event['eventType'] == 'deployment_failed' and event[
                'id'] == deployment_id:
            rollback(marathon_endpoint, deployment_id)
            return False
        print "Post app successful"
    except requests.exceptions.RequestException as e:
        print "Post app failed due to exception"
        raise
    finally:
        unsubscribe(args.marathon_endpoint, get_callback_url())
    return True


def restart_app(args):

    try:
        subscribe(args.marathon_endpoint, get_callback_url())
        restart_response = requests.post(
            args.marathon_endpoint +
            "/v2/apps/{0}/restart".format(args.app_id),
            headers={'Content-Type': 'application/json'})
        restart_response.raise_for_status()
        restart_response_dict = json.loads(restart_response.text)
        deployment_id = restart_response_dict['deploymentId']
        event = event_queue.get()
        if event['eventType'] == 'deployment_failed' and event[
                'id'] == deployment_id:
            rollback(args.marathon_endpoint, deployment_id)
            return False
        print "Restart app successful"
    except requests.exceptions.RequestException as e:
        print "App restart failed due to exception"
        raise
    finally:
        unsubscribe(args.marathon_endpoint, get_callback_url())
    return True


'''Strictly for reference, use it at your own risk.
With great power comes great responsibility'''


def delete_app(args):
    try:
        delete_app_response = requests.delete(
            args.marathon_endpoint + "/v2/apps/{0}".format(args.app_id))
    except requests.exceptions.RequestException as e:
        print "App delete failed"
        raise
    delete_app_response.raise_for_status()


def create_app_wrapper(args):
    app_json = ""
    if args.json_file:
        with open(args.json_file, 'rb') as f:
            app_json = f.read()
            post_app(args.marathon_endpoint, app_json)


def scale_app(args):
    if args.instances < 0:
        raise ValueError('Number of instances should be greater than 0')
    payload = {"instances": args.instances}
    put_app(args, payload)
    print "Scale app successful"


def destroy_app(args):
    delete_app(args)
    print "Destroy app successful"


def deploy_app(args):
    git = gitlab.Gitlab("https://gitlab.corp.olacabs.com",
                        token=args.private_token)
    #git.login(email=args.email, password=args.password)

    app_project = git.getproject(args.app_repo)
    config_project = git.getproject(args.config_repo)

    if args.no_test:
        app_project_branch = 'master'
    else:
        app_project_branch = 'develop'
    latest_commit_app = git.getrepositorybranch(
        app_project['id'], app_project_branch)['commit']['id']
    latest_commit_config = git.getrepositorybranch(config_project['id'],
                                                   'develop')['commit']['id']
    print "Latest app commit = {0}, latest config commit = {1}".format(
        latest_commit_app, latest_commit_config)

    app_id = args.app_name + "." + latest_commit_config
    print "app_id={0}".format(app_id)
    appNameExists = appIdExists = appVersionExists = None
    try:
        appsResponse = get_app(
            args.marathon_endpoint,
            label="{0}=={1}".format("app-name", args.app_name))
        if 'apps' in appsResponse and appsResponse['apps']:
            appNameExists = appsResponse['apps'][0]['id']
    except:
        pass
    try:
        appIdExists = get_app(args.marathon_endpoint, app_id=app_id)
    except:
        pass
    try:
        appVersionExists = get_app(
            args.marathon_endpoint,
            labels="{0}=={1}&{2}=={3}".format(
                "app-name", app_name, "app-version", latest_commit_app))
    except:
        pass
    print("appNameExists={0}, appIdExists={1}, appVersionExists={2}".format(
        appNameExists, appIdExists, appVersionExists))
    if not appNameExists:
        # The app is being deployed for the first time
        # We always pick marathon config file from develop branch
        app_json = base64.b64decode(git.getfile(config_project[
            'id'], args.json_file, 'develop')['content'])
        app_json_dict = json.loads(app_json)
        app_json_dict['id'] = '/' + app_id

        if 'uris' not in app_json_dict:
            app_json_dict['uris'] = ["file:///root/.dockercfg"]
        else:
            app_json_dict['uris'].append("file:///root/.dockercfg")
        if 'labels' not in app_json_dict:
            app_json_dict['labels'] = {"app-name": args.app_name,
                                       "app-version": latest_commit_app,
                                       "config-version": latest_commit_config}
        else:
            app_json_dict['labels']['app-name'] = args.app_name
            app_json_dict['labels']['app-version'] = latest_commit_app
            app_json_dict['labels']['config-version'] = latest_commit_config

        updated_app_json = json.dumps(app_json_dict, ensure_ascii=False)
        post_app(args.marathon_endpoint, updated_app_json)
    elif not appIdExists:
        # app name is present but app id is not
        # so config version has changed
        # get config file from config repo
        app_json = base64.b64decode(git.getfile(config_project[
            'id'], args.json_file, 'develop')['content'])
        app_json_dict = json.loads(app_json)
        print app_json_dict
        new_app_id = '/' + app_id
        old_app_id = appNameExists
        # set id of app to app_id
        app_json_dict['id'] = new_app_id

        # set number of instances to 0, initially
        if 'instances' not in app_json_dict:
            new_instances = 1
        else:
            new_instances = app_json_dict['instances']
        old_instances = appNameExists['apps'][0]['instances']
        if 'uris' not in app_json_dict:
            app_json_dict['uris'] = ["file:///root/.dockercfg"]
        else:
            app_json_dict['uris'].append("file:///root/.dockercfg")

        app_json_dict['instances'] = 0
        labels = {}
        if 'labels' in app_json_dict:
            labels = app_json_dict['labels']
        # update config-version in labels
        labels['app-name'] = args.app_name
        labels['app-version'] = latest_commit_app
        labels['config-version'] = latest_commit_config
        app_json_dict['labels'] = labels
        updated_app_json = json.dumps(app_json_dict, ensure_ascii=False)
        new_instances_cur = 0
        old_instances_cur = old_instances
        x = min(new_instances, old_instances)
        # create a new app
        res = post_app(args.marathon_endpoint, updated_app_json)
        if not res:
            # abort deployment
            return
        while new_instances_cur < x:
            # scale up new app by one
            x = put_app(args.marathon_endpoint, new_app_id,
                        {"instances": new_instances_cur + 1})
            if not x:
                # abort deployment
                break
            # scale down old app by one
            y = put_app(args.marathon_endpoint, old_app_id,
                        {"instances": old_instances_cur - 1})
            old_instances_cur -= 1
            new_instances_cur += 1
        if new_instances_cur < x:
            # Remove new app altogether
            delete_app(Namespace(marathon_endpoint=args.marathon_endpoint,
                                 app_id=new_app_id))
            # Scale up old app
            put_app(args.marathon_endpoint, old_app_id, {"instances":
                                                         old_instances})
            return False

        while old_instances_cur > 0:
            put_app(args.marathon_endpoint, old_app_id,
                    {"instances": old_instances_cur - 1})
            old_instances_cur -= 1

        while new_instances_cur < new_instances:
            put_app(args.marathon_endpoint, new_app_id,
                    {"instances": new_instances_cur + 1})
            new_instances_cur += 1
        #delete_app(Namespace(marathon_endpoint=args.marathon_endpoint, app_id=))
        return True
    elif not appVersionExists:
        # app version has changed, but config version hasn't
        pom = BeautifulSoup(
            base64.b64decode(git.getfile(app_project[
                'id'], 'pom.xml', app_project_branch)['content']), "lxml-xml")
        artifactId = pom.project.artifactId.string
        version = pom.project.version.string
        labels = {}
        if 'labels' in appIdExists['app']:
            labels = appIdExists['app']['labels']
        labels['app-version'] = latest_commit_app

        container = appIdExists['app']['container']
        container['docker']['image'] = "{0}/{1}:{2}".format(
            args.docker_registry, artifactId, version)
        delta_config = {}
        delta_config['container'] = container
        delta_config['labels'] = labels
        put_app(
            Namespace(marathon_endpoint=args.marathon_endpoint,
                      app_id=app_id),
            delta_config)
        return True
    else:
        # app already exists so do a rolling restart
        restart_app(Namespace(marathon_endpoint=args.marathon_endpoint,
                              app_id=app_id))
        return True


if __name__ == '__main__':
    server = setup_httpserver()
    parser = argparse.ArgumentParser(
        description="A command-line client for management of marathon apps")

    parser.add_argument('-m',
                        '--marathon-endpoint',
                        required=True,
                        help="HTTP Endpoint for marathon")
    subparsers = parser.add_subparsers(help="Action to do")
    parser_create = subparsers.add_parser('create', help="Create a new app")
    parser_create.add_argument('--json-file',
                               required=True,
                               help="Path to the marathon json file")
    parser_create.set_defaults(func=create_app_wrapper)

    parser_destroy = subparsers.add_parser('destroy', help="Destroy an app")

    parser_scale = subparsers.add_parser('scale',
                                         help="Scale up or down a running app")
    parser_scale.add_argument('-n',
                              '--instances',
                              type=int,
                              default=1,
                              help="Number of instances of the app")

    parser_restart = subparsers.add_parser('restart',
                                           help="Rolling restart an app")
    parser_restart.add_argument(
        '-t',
        '--timeout',
        type=int,
        default=60,
        help="Maximum time in seconds for the app to restart")

    parser_deploy = subparsers.add_parser('deploy', help="Deploy an app")
    parser_deploy.add_argument('--app-name',
                               required=True,
                               help="Name of the app")
    parser_deploy.add_argument(
        '--app-repo',
        required=True,
        help="Gitlab repo name for the app - namespace/project_name")
    parser_deploy.add_argument(
        '--config-repo',
        required=True,
        help="Gitlab repo name for the app config - namespace/project_name")
    parser_deploy.add_argument('-e', '--email', help="Gitlab Email id")
    #parser_deploy.add_argument('-p', '--password', help="Gitlab Password")
    parser_deploy.add_argument('-t',
                               '--private-token',
                               help="Gitlab Private Token")
    parser_deploy.add_argument('-d',
                               '--docker-registry',
                               help="Docker registry hostname and port")
    parser_deploy.add_argument(
        '--no-test',
        action='store_true',
        help=
        '''If this flag is passed, latest release version of app is deployed.
        Otherwise, latest snapshot version of app is deployed. By default, latest snapshot version is used''')
    parser_deploy.add_argument(
        '--json-file',
        required=True,
        help="Path for the marathon json file in the config git repo")
    parser_deploy.set_defaults(no_test=False)
    parser_deploy.set_defaults(func=deploy_app)

    for subparser, function in [(parser_destroy, destroy_app),
                                (parser_scale, scale_app),
                                (parser_restart, restart_app)]:
        subparser.add_argument('-id',
                               '--app-id',
                               required=True,
                               help="Name of the marathon app")
        subparser.set_defaults(func=function)

    args = parser.parse_args()

    try:
        args.func(args)
    except Exception, err:
        server.shutdown()
        print traceback.format_exc()
