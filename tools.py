
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymysql import connect, MySQLError, cursors

from configurable import Configurable


class APIRequest():

    def __init__(self, api_key, version='v3'):
        self.key = api_key
        self.build = build('youtube', version, developerKey=api_key)
        self.collection = {'videos': self.build.videos(), 'channels': self.build.channels(),
                           'playlistItems': self.build.playlistItems(), 'commentThreads': self.build.commentThreads(),
                           'search': self.build.search()}

    def list(self, collection, **fields):
        try:
            request = self.collection[collection].list(**fields)
            return request, request.execute()
        except KeyError as err:
            raise Exception('(tools.py) KeyError while accessing API: {}'.format(repr(err)))
        except HttpError as err:
            raise Exception('(tools.py) HttpError while accessing API: {}'.format(repr(err)))

    def list_next(self, collection, request=None, response=None):
        if request and response:
            try:
                request = self.collection[collection].list_next(request, response)
                if request:
                    return request, request.execute()
            except KeyError as err:
                raise Exception('(tools.py) KeyError while accessing API: {}'.format(repr(err)))
            except HttpError as err:
                raise Exception('(tools.py) HttpError while accessing API: {}'.format(repr(err)))
        return None, None


class Database(Configurable):

    def __init__(self):
        super().__init__(fields=['database'])

    def select(self, table, *columns, where=None):
        result = []
        # try:
        #     fields = next(t['fields'] for t in self.config['database']['tables'] if t['name'] == table)
        # except StopIteration as err:
        #     raise Exception('(tools.py) KeyError while accessing database: Invalid table. {}'.format(repr(err)))
        # if columns and not all(c in fields for c in columns):
        #     raise Exception('(tools.py) KeyError while accessing database: Invalid columns.')
        if not columns:
            columns = ['*']
        try:
            conn = connect(**self.config['database']['connection'])
            try:
                with conn.cursor(cursors.DictCursor) as cursor:
                    query = {'table': table, 'columns': ', '.join(columns), 'where': ''}
                    if where:
                        query['where'] = 'WHERE {}'.format(' AND '.join(where))
                    sql = 'SELECT %(columns)s from %(table)s %(where)s' % query
                    cursor.execute(sql)
                    result = cursor.fetchall()
            except MySQLError as err:
                raise Exception(repr(err))
            finally:
                conn.close()
        except MySQLError as err:
            raise Exception('(tools.py) MySQLError while accessing API: {}'.format(repr(err)))
        return result

    def insert(self, table, values):
        many = False
        columns = []
        last_id = 0
        data = None
        try:
            fields = next(t['fields'] for t in self.config['database']['tables'] if t['name'] == table)
        except StopIteration as err:
            raise Exception('(tools.py) KeyError while accessing database: Invalid table. {}'.format(repr(err)))
        if isinstance(values, list):
            many = True
            data = []
            for v in values:
                columns = list(v.keys())
                if not all(k in fields for k in columns):
                    raise Exception('(tools.py) KeyError while accessing database: Invalid columns.')
                data.append(tuple(v[c] for c in columns))

        else:
            columns = list(values.keys())
            if not all(k in fields for k in values.keys()):
                raise Exception('(tools.py) KeyError while accessing database: Invalid columns.')
            data = tuple([values[c] for c in columns])
        try:
            conn = connect(**self.config['database']['connection'])
            try:
                with conn.cursor() as cursor:
                    query = {'table': table, 'columns': ', '.join(columns), 'values': ', '.join(['%s'] * len(columns))}
                    sql = 'INSERT INTO %(table)s (%(columns)s) VALUES (%(values)s)' % query
                    if many:
                        cursor.executemany(sql, data)
                    else:
                        cursor.execute(sql, data)
                    conn.commit()
                    last_id = cursor.lastrowid
            except MySQLError as err:
                raise Exception(repr(err))
            finally:
                conn.close()
        except MySQLError as err:
            raise Exception('(tools.py) MySQLError while accessing database: {}'.format(repr(err)))
        return last_id
