"""
Author: Leo Vidarte <http://nerdlabs.com.ar>

This is free software,
you can redistribute it and/or modify it
under the terms of the GPL version 3
as published by the Free Software Foundation.

"""

import os
import json
import pymysql
from pymongo import MongoClient

from log import logger


class Syncer:

    PROCESSING = 'processing'
    PROCESSED  = 'processed'
    ERROR      = 'error'
    FMT_DATE   = '%Y-%m-%d %H:%M:%S'

    def __init__(self, config, max_events=5):
        self.config = config
        self.max_events = max_events
        self.pid = os.getpid()
        self.db = self.get_mongo_db()
        self.conn = self.get_mysql_conn()

    def get_mongo_db(self):
        return MongoClient(self.config.MONGO_URI)[self.config.MONGO_DB]

    def get_mysql_conn(self):
        return pymysql.connect(
            port=3306,
            host=self.config.MYSQL_HOST,
            user=self.config.MYSQL_USER,
            passwd=self.config.MYSQL_PASS,
            db=self.config.MYSQL_NAME,
            cursorclass=pymysql.cursors.DictCursor)

    def update_event(self, event):
        try:
            with self.conn.cursor() as cur:
                sql = """
                    UPDATE db_activity
                    SET status = %s, message = %s
                    WHERE id = %s
                """
                args = (event['status'], event['message'], event['id'])
                result = cur.execute(sql, args)
                self.conn.commit()
        except:
            result = 0
        return bool(result)

    def reserve_events(self, event_type):
        try:
            with self.conn.cursor() as cur:
                sql = """
                    UPDATE db_activity
                    SET status = %s, message = %s
                    WHERE type = %s AND
                          status = 'pending' AND
                          message is null
                    ORDER BY created_at ASC LIMIT %s
                """
                args = (self.PROCESSING, str(self.pid),
                        event_type, self.max_events)
                result = cur.execute(sql, args)
                self.conn.commit()
        except:
            result = 0
        return bool(result)

    def get_events(self, event_type):
        try:
            with self.conn.cursor() as cur:
                sql = """
                    SELECT * FROM db_activity
                    WHERE type = %s AND
                          status = %s AND
                          message = %s
                """
                args = (event_type, self.PROCESSING, str(self.pid))
                cur.execute(sql, args)
                events = [self.parse_event(row) for row in cur.fetchall()]
        except:
            events = []
        return events

    def parse_event(self, row):
        row['updated_at'] = row['updated_at'].strftime(self.FMT_DATE)
        row['created_at'] = row['created_at'].strftime(self.FMT_DATE)
        row['data'] = json.loads(row['data'])
        return row

    def process(self):
        self.reserve_events('stock')
        for event in self.get_events('stock'):
            event = self.update_stock(event)
            self.update_event(event)
            logger.info({'action': 'event_process',
                         'pid': self.pid,
                         'event': event})

    def clean(self):
        self.conn.close()

    def update_stock(self, event):
        _filter = {
            'models': {
                '$elemMatch': {
                    'externalId': event['data']['id'],
                    'updatedAt': {'$lt': event['updated_at']}
                }
            }
        }
        update = {
            '$inc': {
                'models.$.stock': event['data']['stock_movement'],
                'stock': event['data']['stock_movement']
            },
            '$set': {
                'models.$.updatedAt': event['updated_at'],
            }
        }
        try:
            self.db.Items.update_many(_filter, update)
            event['status'] = self.PROCESSED
            event['message'] = 'OK'
        except Exception as e:
            event['status'] = self.ERROR
            event['message'] = e.message
        return event

