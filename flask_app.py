import os, sys, time, random, base64
from pprint import pprint
from flask import Flask,render_template,json,request,redirect,session,jsonify
from flaskext.mysql import MySQL
from google.cloud import pubsub
from oauth2client.client import GoogleCredentials

app = Flask(__name__)

#db settings
app.config['MYSQL_DATABASE_USER'] = 'your db user'
app.config['MYSQL_DATABASE_PASSWORD'] = 'your db password'
app.config['MYSQL_DATABASE_DB'] = 'your db schema name'
app.config['MYSQL_DATABASE_HOST'] = 'your db host'

#pubsub related
app.config['SUBSCRIPTION_NAME'] = 'subscription name given by company manager'
app.config['PROJECT_ID'] = 'project id given by company manager'
mysql = MySQL(app)

@app.route('/')
def appInit():
    conn = mysql.connect()
    cursor = conn.cursor()
    sql = """
        SELECT      *
        FROM        `pubsub`
        order by    id
        desc limit  100
    """
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('pubsub.html',title='Pub/Sub Entries Example',data=data)

@app.route('/pubSubListener', methods=['POST', 'GET'])
def pubSubListener():
    if request.method == 'POST':
        jsonMsg = request.get_json(silent=True,force=True)
    else:
        #if get request send example data in.
        jsonStr = '{ "message": {"attributes": {"key": "value"},"data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==","message_id": "136969346945"},"subscription": "projects/myproject/subscriptions/mysubscription"}'
        jsonMsg = json.loads(jsonStr)
    output = getMsg(jsonMsg)
    return output

@app.route('/pubSubWorker', methods=['POST', 'GET'])
def pubSubWorker():
    credentials = GoogleCredentials.get_application_default()
    subscriber = pubsub.SubscriberClient()
    subscription = subscriber.subscribe(
        'projects/'+app.config['PROJECT_ID']+'/subscriptions/'+app.config['SUBSCRIPTION_NAME'],
    )
    future = subscription.open(callback)
    try:
        future.result()
    except Exception as ex:
        subscription.close()
        raise

    return "{status:'finished'}"

def callback(message):
    message.ack()
    insertIntoDb(message.data.decode("utf-8"))
    return message.data

def insertIntoDb(msg):
    conn = mysql.connect()
    cursor = conn.cursor()

    sql = """
        INSERT INTO `pubsub`
            (`data`)
        VALUES
            (%s)
    """
    args = [msg,]
    cursor.execute(sql, args)
    conn.commit()
    cursor.close()
    conn.close()
    return

def getMsg(jsonMsg):
    msgData = base64.b64decode(jsonMsg['message']['data'])
    insertIntoDb(msgData)
    return msgData

if __name__ == '__main__':
    app.run(debug=True)
