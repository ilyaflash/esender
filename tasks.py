# -*- coding: utf-8 -*-
#to start from terminal: "celery -A tasks worker --autoscale=1,2 --loglevel=info"

#Libs
from celery import Celery
import smtplib
from json import loads

#Instanse
app = Celery('tasks', broker='redis://guest@localhost//')

#Task def
@app.task(bind=True, retry_kwargs={'max_retries': 2})
def send_mail_from_app4hiiigmailcom(self, send_conf):
    try:
        server = smtplib.SMTP_SSL(host='smtp.gmail.com', port=465)
        server.login(user='app4hiii@gmail.com', password='bc8yqrevyud')
        msg = 'Subject: {}\n\n{}'.format(send_conf['subject'], send_conf['msg']).encode('utf-8')
        status = server.sendmail(from_addr='app4hiii@gmail.com', to_addrs=send_conf['to_addrs'], msg=msg)
        server.close()
        if status:
            self.retry(countdown=2)
    except:
        self.retry(countdown=2)

#Send_mail_to_que def
def send_mail_to_que(path='mails.json'):    
    def get_json_confs(path='mails.json'):
        with open(path, 'r') as file:
            return loads(file.read(), encoding='utf-8')
    return [send_mail_from_app4hiiigmailcom.delay(i) for i in get_json_confs(path=path)]         #to que

#Run send_mail_to_que
send_mail_to_que()
