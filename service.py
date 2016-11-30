import pika
import json
import dataset
import pymysql
pymysql.install_as_MySQLdb()

credentials = pika.PlainCredentials('server1_dcm', '8nfdsS12gaf')

server2_vhost_connection = pika.BlockingConnection(pika.ConnectionParameters(
    virtual_host = '/server2', credentials = credentials))
server2_vhost_channel = server2_vhost_connection.channel()
server2_vhost_channel.queue_declare(queue = 'from_agent_to_middleware', durable = True)

server3_vhost_connection = pika.BlockingConnection(pika.ConnectionParameters(
    virtual_host = '/server3', credentials = credentials))
server3_vhost_channel = server3_vhost_connection.channel()
server3_vhost_channel.queue_declare(queue = 'from_agent_to_middleware', durable = True)

server4_vhost_connection = pika.BlockingConnection(pika.ConnectionParameters(
    virtual_host = '/server4', credentials = credentials))
server4_vhost_channel = server4_vhost_connection.channel()
server4_vhost_channel.queue_declare(queue = 'from_agent_to_middleware', durable = True)


def from_agent_to_middleware_callback(ch, method, properties, body):
    decoded_json = json.loads(body.decode('utf-8'))
    db = dataset.connect('mysql://dcm_user:dcmUser@1115@localhost/wsato_qiligeer')
    table = db['domains']
    result = table.find_one(name = decoded_json['name'])

    dic = dict(id = result['id'], name = decoded_json['name'])
    if 'status' in decoded_json:
        dic['status'] = decoded_json['status']
    if 'sshkey_path' in decoded_json:
        dic['sshkey_path'] = decoded_json['sshkey_path']
    if 'ipv4_address' in decoded_json:
        dic['ipv4_address'] = decoded_json['ipv4_address']
    if 'error' in decoded_json:
        dic['error'] = decoded_json['error']

    if len(dic) < 3:
        return

    db.begin()
    try:
        table.update(dic, ['id', 'name'])
        db.commit()
    except:
        db.rollback()

server2_vhost_channel.basic_consume(from_agent_to_middleware_callback,
                      queue = 'from_agent_to_middleware',
                      no_ack = True)
server2_vhost_channel.start_consuming()

server3_vhost_channel.basic_consume(from_agent_to_middleware_callback,
                      queue = 'from_agent_to_middleware',
                      no_ack = True)
server3_vhost_channel.start_consuming()

server4_vhost_channel.basic_consume(from_agent_to_middleware_callback,
                      queue = 'from_agent_to_middleware',
                      no_ack = True)
server4_vhost_channel.start_consuming()
