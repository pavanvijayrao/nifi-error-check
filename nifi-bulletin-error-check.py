"""
Monitors Nifi Bulletin Board for processor or controller errors
arguments: nifi url, client certificate path
"""

import nipyapi
import logging
import requests
import os

nifi_url = os.getenv("NIFI_URL")
client_cert_file = os.getenv("CLIENT_CERT_FILE_PATH")
client_key_file = os.getenv("CLIENT_KEY_FILE_PATH")
ca_file = os.getenv("DIGICERT_FILE_PATH")
slack_url = os.getenv("SLACK_WEBHOOK_URL")
slack_url_2 = os.getenv("SLACK_WEBHOOK_URL_NIFI_ALERT")
env = os.getenv("ENV_NAME")
notify_errors_list = os.getenv("NIFI_NOTIFY_ERRORS_LIST")
notify_file_errors_list = os.getenv("NIFI_NOTIFY_FILE_ERRORS_LIST")

logger = logging.getLogger('monitorBulletinError')
logger.setLevel(logging.DEBUG)

errors = dict()
file_errors = dict()
msg = ''
parent_flow = ''

# SSL & Authentication
nipyapi.utils.set_endpoint(nifi_url)
nipyapi.nifi.configuration.verify_ssl = True

nipyapi.security.set_service_ssl_context(service='nifi',
                                         ca_file=ca_file,
                                         client_cert_file=client_cert_file,
                                         client_key_file=client_key_file,
                                         client_key_password=None)

def getParentFlowName(group_id):
    pg = nipyapi.canvas.get_process_group(group_id, identifier_type='id')
    if pg is not None:
        if pg.component.parent_group_id is not None:
            pg = nipyapi.canvas.get_process_group(pg.component.parent_group_id, identifier_type='id')
            return pg.component.name

def sendAlerts():
    response = nipyapi.canvas.get_bulletin_board()
    bulletins = response.bulletin_board.bulletins

    flowList = []
    for bulletin in bulletins:
        group_id = bulletin.group_id
        parent_flow = getParentFlowName(group_id)
        if not flowList.__contains__(parent_flow):
            flowList.append(parent_flow)
    notify_list = str(notify_errors_list).split("|")
    file_errors_list = str(notify_file_errors_list).split("|")

    for flow in flowList:
        for bulletin in bulletins:
            source_name = bulletin.bulletin.source_name
            group_id = bulletin.group_id
            parent_flow = getParentFlowName(group_id)
            msg = ' Processor/Controller:*' + str(source_name) + '*, Error message:' + bulletin.bulletin.message + ', Timestamp:' + bulletin.bulletin.timestamp
            message = msg.replace("\n", " ")
            message = message.replace("\"", "\\\"")
            for error in notify_list:
                if message.__contains__(str(error)):
                    if source_name.__contains__("PublishKafka"):
                        source_name = "PublishKafkaRecord"
                    errors[parent_flow+'|'+source_name] = message
            for error in file_errors_list:
                if message.__contains__(str(error)):
                    if source_name.__contains__("PublishKafka"):
                        source_name = "PublishKafkaRecord"
                    file_errors[parent_flow+'|'+source_name] = message

    for item in errors:
        keys = str(item).split('|')
        parent_flow = keys[0]
        username = env + '-' + parent_flow + '-Nifi-Errors'
        data = '{\"username\":\"' + username + '\", \"text\":\"' + errors[item] + '\"}'
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        print(str(len(errors)) + " error(s) found...sending slack alert...")
        slack_resp = requests.post(slack_url, headers=headers, verify=False, data=data)
        print("Slack response code:" + str(slack_resp.status_code))

    for item in file_errors:
        keys = str(item).split('|')
        parent_flow = keys[0]
        username = env + '-' + parent_flow + '-Nifi-Errors'
        data = '{\"username\":\"' + username + '\", \"text\":\"' + file_errors[item] + '\"}'
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        print(str(len(file_errors)) + " error(s) found...sending slack alert...")
        slack_resp = requests.post(slack_url_2, headers=headers, verify=False, data=data)
        print("Slack response code:" + str(slack_resp.status_code))

if __name__ == '__main__':
    sendAlerts()
