import pika
import json
import dataset
import pymysql
pymysql.install_as_MySQLdb()

credentials = pika.PlainCredentials('server1_dcm', '8nfdsS12gaf')
connection  = pika.BlockingConnection(pika.ConnectionParameters(
    virtual_host = '/server2', credentials = credentials))
channel = connection.channel()
channel.queue_declare(queue = 'from_agent_to_middleware', durable = True)

def from_agent_to_middleware_callback(ch, method, properties, body):
    decoded_json = json.loads(body.decode('utf-8'))
    db = dataset.connect('mysql://dcm_user:dcmUser@1115@127.0.0.1/wsato_qiligeer')
    table = db['domains']
    result = table.find_one(name = decoded_json['name'])

    # TODO 投げられたデータに対して update する対象を変える
    # TODO update する対象の変更？

    db.begin()
    try:
        table.update(dict(id = result['id'], name = decoded_json['name'], status = decoded_json['status']), ['id', 'name'])
        db.commit()
    except:
        db.rollback()
        pass

channel.basic_consume(from_agent_to_middleware_callback,
                      queue = 'from_agent_to_middleware',
                      no_ack = True)

channel.start_consuming()

# TODO Add server3 and server 4
