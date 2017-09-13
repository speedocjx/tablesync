#!/bin/env python
#-*-coding:utf-8-*-
import pika,MySQLdb,argparse,sys

class Receiver(object):

    def __init__(self, startFile=None, startPos=None, endFile=None, endPos=None, ramq_username=None,ramq_pwd=None,ramq_ip=None,queue_name=None,dest_host=None,dest_port=None,dest_dbname=None,dest_user=None,dest_passwd=None):
        self.dest_host = dest_host
        self.dest_port = dest_port
        self.dest_dbname = dest_dbname
        self.dest_user = dest_user
        self.dest_passwd = dest_passwd
        self.lastpos = 4
        self.startFile = startFile
        self.startPos = startPos if startPos else 4 # use binlog v4
        self.endFile = endFile if endFile else startFile
        self.endPos = endPos
        self.flag = False
        self.ramq_username = ramq_username #定远程rabbitmq的用户名密码
        self.ramq_pwd = ramq_pwd
        self.queue_name = queue_name
        self.ramq_ip = ramq_ip
        self.user_pwd = pika.PlainCredentials(self.ramq_username, self.ramq_pwd)
        self.s_conn = pika.BlockingConnection(pika.ConnectionParameters(self.ramq_ip, credentials=self.user_pwd))#创建连接
        self.chan = self.s_conn.channel()
        self.conn = MySQLdb.connect(host=self.dest_host, user=self.dest_user, passwd=self.dest_passwd, port=int(self.dest_port), connect_timeout=5, charset='utf8')
        self.conn.select_db(self.dest_dbname)
        self.cursor = self.conn.cursor()
    def mysql_exec(self,sql,param=''):
        try:
            if param <> '':
                self.cursor.execute(sql, param)
            else:
                self.cursor.execute(sql)
            self.conn.commit()
        except Exception,e:
            print e
    def _close(self):
        self.cursor.close()
        self.conn.close()


    def callback(self,ch,method,properties,body): #定义一个回调函数，用来接收生产者发送的消息
        binlogpos = body[body.rindex('$')+1:]
        pos = int(binlogpos[binlogpos.rindex('-')+1:])
        binfile = binlogpos[:binlogpos.rindex('-')]
        if self.startFile and self.startPos and self.flag==False:
            if binfile == self.startFile and pos>=self.startPos or binfile>self.startFile:
                self.flag = True
                print "begin apply now !"
            else :
                self.flag =False
        else:
            self.flag =True
        if self.endFile and self.endPos:
            if (binfile == self.endFile and pos > self.endPos) or binfile > self.endFile :
                print "at the end"
                self.flag = False
                sys.exit(1)
        if self.flag:
            print binlogpos
            print body[:body.rindex('$$')]
            self.mysql_exec(body[:body.rindex('$$')])
        self.lastpos=binlogpos[1]


    def run(self):
        self.chan.queue_declare(queue=self.queue_name)
        self.chan.basic_consume(self.callback,  #调用回调函数，从队列里取消息
                           queue=self.queue_name,#指定取消息的队列名
                           no_ack=True) #取完一条消息后，不给生产者发送确认消息，默认是False的，即  默认给rabbitmq发送一个收到消息的确认，一般默认即可
        print('[消费者] waiting for msg .')
        self.chan.start_consuming()#开始循环取消息
    def __del__(self):
        self._close()
def parse_args(args):
    """parse args for binlog2sql"""

    parser = argparse.ArgumentParser(description='Get sql from mq and Apply', add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h','--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str,
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)

    rabbitmq_setting = parser.add_argument_group('rabbitmq setting')
    rabbitmq_setting.add_argument( '--mquser', dest='mquser', type=str,
                                 help='MQ Username to log in as', default='guest')
    rabbitmq_setting.add_argument( '--mqhost', dest='mqhost', type=str,
                                 help='Host the MQ server located', default='127.0.0.1')
    rabbitmq_setting.add_argument('--mqpassword', dest='mqpassword', type=str,
                                 help='MQ Password to use', default='guest')
    rabbitmq_setting.add_argument( '--qname', dest='qname', type=str,
                                 help='MQ queue name to use', default='')
    range = parser.add_argument_group('range filter')
    range.add_argument('--start-file', dest='startFile', type=str,
                       help='Start binlog file to be applied')
    range.add_argument('--start-position', '--start-pos', dest='startPos', type=int,
                       help='Start position of the --start-file', default=4)
    range.add_argument('--stop-file', '--end-file', dest='endFile', type=str,
                       help="Stop binlog file to be applied. default: '--start-file'", default='')
    range.add_argument('--stop-position', '--end-pos', dest='endPos', type=int,
                       help="Stop position of --stop-file. default: latest position of '--stop-file'", default=0)

    parser.add_argument('--stop-never', dest='stopnever', action='store_true',
                        help='Wait for more data from the server. default: stop replicate at the last binlog when you start binlog2sql', default=False)

    parser.add_argument('--help', dest='help', action='store_true', help='help infomation', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--database', dest='database', type=str,
                        help='destination db', default='')
    schema.add_argument('-t', '--table', dest='table', type=str,
                        help='tables you want to apply', default='')
    return parser

def command_line_args(args):
    parser = parse_args(args)
    args = parser.parse_args(args)
    if args.help:
        parser.print_help()
        sys.exit(1)
    return args

if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    print args
    rece = Receiver( ramq_username=args.mquser,ramq_pwd=args.mqpassword,ramq_ip=args.mqhost,
                     queue_name=args.qname,dest_host=args.host, startFile=args.startFile,
                     startPos=args.startPos, endFile=args.endFile, endPos=args.endPos,
                     dest_port=args.port,dest_dbname=args.database,dest_user=args.user,dest_passwd=args.password)
    rece.run()
# python mysqlreceiver.py     -h192.168.3.200 -P3306 -uchang -p111111 -ddest --mquser=chang --mqhost=192.168.3.200 \
# --start-file=mysql-bin.000024 --start-position=2292 --stop-file=mysql-bin.000024 --stop-position=4322 --mqpassword=111111 --qname=chang-test
