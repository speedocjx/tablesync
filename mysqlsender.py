#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, datetime,argparse,time
import pymysql
import pika
from multiprocessing import Process;



from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent



class Binlog2sql(object):

    def __init__(self, connectionSettings, startFile=None, startPos=None, endFile=None, endPos=None, startTime=None,
                 stopTime=None, only_schemas=None, only_tables=None, nopk=False, flashback=False, stopnever=True):
        '''
        connectionSettings: {'host': 127.0.0.1, 'port': 3306, 'user': slave, 'passwd': slave}
        '''
        if not startFile:
            raise ValueError('lack of parameter,startFile.')
        self.ramq_ip = '192.168.3.200'
        self.ramq_username = 'chang'   #指定远程rabbitmq的用户名密码
        self.ramq_pwd = '111111'
        self.user_pwd = pika.PlainCredentials(self.ramq_username, self.ramq_pwd)
        self.s_conn = pika.BlockingConnection(pika.ConnectionParameters(self.ramq_ip,connection_attempts=3, credentials=self.user_pwd,heartbeat_interval=0))#创建连接
        self.chan = self.s_conn.channel()  #在连接上创建一个频道

        self.connectionSettings = connectionSettings
        self.startFile = startFile
        self.startPos = startPos if startPos else 4 # use binlog v4
        self.endFile = endFile if endFile else startFile
        self.endPos = endPos
        self.startTime = datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S") if startTime else datetime.datetime.strptime('1970-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        self.stopTime = datetime.datetime.strptime(stopTime, "%Y-%m-%d %H:%M:%S") if stopTime else datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        if self.only_tables :
            for i in self.only_tables :
                self.chan.queue_declare(queue=self.only_schemas[0] + '-' + i ) #声明队列，生产者和消费者都要声明一个相同的队列，用来防止万一某一方挂了，另一方能正常运行
        self.nopk, self.flashback, self.stopnever = (nopk, flashback, stopnever)
        self.sqllist = []
        self.binlogList = []
        self.connection = pymysql.connect(**self.connectionSettings)
        try:
            cur = self.connection.cursor()
            cur.execute("SHOW MASTER STATUS")
            self.eofFile, self.eofPos = cur.fetchone()[:2]
            cur.execute("SHOW MASTER LOGS")
            binIndex = [row[0] for row in cur.fetchall()]
            if self.startFile not in binIndex:
                raise ValueError('parameter error: startFile %s not in mysql server' % self.startFile)
            binlog2i = lambda x: x.split('.')[1]
            for bin in binIndex:
                if binlog2i(bin) >= binlog2i(self.startFile) and binlog2i(bin) <= binlog2i(self.endFile):
                    self.binlogList.append(bin)

            cur.execute("SELECT @@server_id")
            self.serverId = cur.fetchone()[0]
            if not self.serverId:
                raise ValueError('need set server_id in mysql server %s:%s' % (self.connectionSettings['host'], self.connectionSettings['port']))
        finally:
            cur.close()
    def data_events(self,timeout):
        while True:
            time.sleep(timeout)
            self.s_conn.process_data_events()

    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.connectionSettings, server_id=self.serverId,
                                    log_file=self.startFile, log_pos=self.startPos, only_schemas=self.only_schemas,
                                    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent,QueryEvent],
                                    only_tables=self.only_tables, resume_stream=True,blocking=True)

        # eStartPos, lastPos = stream.log_pos, stream.log_pos
        cur = self.connection.cursor()
        for binlogevent in stream:
            if isinstance(binlogevent, QueryEvent):
                pass
                # sql = concat_sql_from_binlogevent(cursor=cur, binlogevent=binlogevent, flashback=self.flashback, nopk=self.nopk)
                # if sql:
                #     print sql
            elif isinstance(binlogevent, WriteRowsEvent) or isinstance(binlogevent, UpdateRowsEvent) or isinstance(binlogevent, DeleteRowsEvent):
                for row in binlogevent.rows:
                    sql = concat_sql_from_binlogevent(cursor=cur, binlogevent=binlogevent, row=row , flashback=self.flashback, nopk=self.nopk)
                    print sql
                    print  '{0}-{1}'.format(stream.log_file,str(stream.log_pos))

                    self.chan.basic_publish(exchange='',  #交换机
                                            routing_key=binlogevent.schema+ '-' + binlogevent.table,#路由键，写明将消息发往哪个队列，本例是将消息发往队列hello
                                            body= '{0}$$binlogpos${1}-{2}'.format(sql,stream.log_file,str(stream.log_pos))) #生产者要发送的消息
            # if not (isinstance(binlogevent, RotateEvent) or isinstance(binlogevent, FormatDescriptionEvent)):
            #         lastPos = binlogevent.packet.log_pos
        self.s_conn.close()#当生产者发送完消息后，可选择关闭连接
        cur.close()
        stream.close()
        return True

    def __del__(self):
        pass
        # self.connection.cursor()



def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False

def parse_args(args):
    """parse args for binlog2sql"""

    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h','--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str,
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)
    range = parser.add_argument_group('range filter')
    range.add_argument('--start-file', dest='startFile', type=str,
                       help='Start binlog file to be parsed')
    range.add_argument('--start-position', '--start-pos', dest='startPos', type=int,
                       help='Start position of the --start-file', default=4)
    range.add_argument('--stop-file', '--end-file', dest='endFile', type=str,
                       help="Stop binlog file to be parsed. default: '--start-file'", default='')
    range.add_argument('--stop-position', '--end-pos', dest='endPos', type=int,
                       help="Stop position of --stop-file. default: latest position of '--stop-file'", default=0)
    range.add_argument('--start-datetime', dest='startTime', type=str,
                       help="Start reading the binlog at first event having a datetime equal or posterior to the argument; the argument must be a date and time in the local time zone, in any format accepted by the MySQL server for DATETIME and TIMESTAMP types, for example: 2004-12-25 11:25:56 (you should probably use quotes for your shell to set it properly).", default='')
    range.add_argument('--stop-datetime', dest='stopTime', type=str,
                       help="Stop reading the binlog at first event having a datetime equal or posterior to the argument; the argument must be a date and time in the local time zone, in any format accepted by the MySQL server for DATETIME and TIMESTAMP types, for example: 2004-12-25 11:25:56 (you should probably use quotes for your shell to set it properly).", default='')
    parser.add_argument('--stop-never', dest='stopnever', action='store_true',
                        help='Wait for more data from the server. default: stop replicate at the last binlog when you start binlog2sql', default=False)

    parser.add_argument('--help', dest='help', action='store_true', help='help infomation', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')

    # exclusive = parser.add_mutually_exclusive_group()
    parser.add_argument('-K', '--no-primary-key', dest='nopk', action='store_true',
                           help='Generate insert sql without primary key if exists', default=False)
    parser.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                           help='Flashback data to start_postition of start_file', default=False)
    return parser

def command_line_args(args):
    parser = parse_args(args)
    args = parser.parse_args(args)
    if args.help:
        parser.print_help()
        sys.exit(1)
    if not args.startFile:
        raise ValueError('Lack of parameter: startFile')
    if args.flashback and args.stopnever:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.nopk:
        raise ValueError('Only one of flashback or nopk can be True')
    if (args.startTime and not is_valid_datetime(args.startTime)) or (args.stopTime and not is_valid_datetime(args.stopTime)):
        raise ValueError('Incorrect datetime argument')
    return args


def compare_items((k, v)):
    #caution: if v is NULL, may need to process
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k

def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value

def concat_sql_from_binlogevent(cursor, binlogevent, row=None, eStartPos=None, flashback=False, nopk=False):
    if flashback and nopk:
        raise ValueError('only one of flashback or nopk can be True')
    if not (isinstance(binlogevent, WriteRowsEvent) or isinstance(binlogevent, UpdateRowsEvent) or isinstance(binlogevent, DeleteRowsEvent) or isinstance(binlogevent, QueryEvent)):
        raise ValueError('binlogevent must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

    sql = ''
    if isinstance(binlogevent, WriteRowsEvent) or isinstance(binlogevent, UpdateRowsEvent) or isinstance(binlogevent, DeleteRowsEvent):
        pattern = generate_sql_pattern(binlogevent, row=row, flashback=flashback, nopk=nopk)
        sql = cursor.mogrify(pattern['template'], pattern['values'])
        # sql += ' #start %s end %s time %s' % (eStartPos, binlogevent.packet.log_pos, datetime.datetime.fromtimestamp(binlogevent.timestamp))
    elif flashback is False and isinstance(binlogevent, QueryEvent) and binlogevent.query != 'BEGIN' and binlogevent.query != 'COMMIT':
        sql += '{0};'.format(fix_object(binlogevent.query))

    return sql

def generate_sql_pattern(binlogevent, row=None, flashback=False, nopk=False):
    template = ''
    values = []
    if isinstance(binlogevent, WriteRowsEvent):
        if nopk:
            # print binlogevent.__dict__
            # tableInfo = (binlogevent.table_map)[binlogevent.table_id]
            # if tableInfo.primary_key:
            #     row['values'].pop(tableInfo.primary_key)
            if binlogevent.primary_key:
                row['values'].pop(binlogevent.primary_key)

        template = 'INSERT INTO `{0}`({1}) VALUES ({2});'.format(
            binlogevent.table,
            ', '.join(map(lambda k: '`%s`'%k, row['values'].keys())),
            ', '.join(['%s'] * len(row['values']))
        )
        values = map(fix_object, row['values'].values())
    elif isinstance(binlogevent, DeleteRowsEvent):
        template ='DELETE FROM `{0}` WHERE {1} LIMIT 1;'.format(
            binlogevent.table,
            ' AND '.join(map(compare_items, row['values'].items()))
        )
        values = map(fix_object, row['values'].values())
    elif isinstance(binlogevent, UpdateRowsEvent):
        template = 'UPDATE `{0}` SET {1} WHERE {2} LIMIT 1;'.format(
            binlogevent.table,
            ', '.join(['`%s`=%%s'%k for k in row['after_values'].keys()]),
            ' AND '.join(map(compare_items, row['before_values'].items()))
        )
        values = map(fix_object, row['after_values'].values()+row['before_values'].values())

    return {'template':template, 'values':values}


if __name__ == '__main__':

    args = command_line_args(sys.argv[1:])
    connectionSettings = {'host':args.host, 'port':args.port, 'user':args.user, 'passwd':args.password}
    binlog2sql = Binlog2sql(connectionSettings=connectionSettings, startFile=args.startFile,
                            startPos=args.startPos, endFile='', endPos=0,
                            startTime='', stopTime='', only_schemas=args.databases,
                            only_tables=args.tables, nopk=False)
    p = Process(target = binlog2sql.data_events, args = (5,))
    p.start()

    binlog2sql.process_binlog()
    p.join()
    #python mysqlsender.py -h192.168.3.200 -P3306 -uchang -p111111 -dchang -ttest --start-file=mysql-bin.000026
