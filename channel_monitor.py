from configurable import Configurable
from csv import DictReader
from datetime import date, datetime, timedelta
from isodate import parse_duration
from itertools import groupby
import logging
from queue import Empty, Queue
from threading import Lock, Thread, Semaphore, get_ident, current_thread
from tools import APIRequest, Database


class Monitor(Configurable):

    def chunks(self, ls, n=0):
        if not n:
            n = self.config['api']['videos']['batchLimit']
        for i in range(0, len(ls), n):
            yield ls[i:i + n]

    def parse_date(self, s,
                   return_datetime=False):  # Converts datetime string from API response to date/datetime object
        d = datetime.strptime(s[:-1].split('.')[0], '%Y-%m-%dT%H:%M:%S') + timedelta(hours=self.config['api']['timezoneDifference'])
        if return_datetime:
            return d
        return d.date()

    def __init__(self, from_file=False):
        super().__init__(self)

        with open(self.config['files']['collectIdFile'], 'r') as f:
            self.collect_id = int(f.readline().strip())
        self.logger = self.create_log()
        self.logger.info('Log initialized.')
        self.channel_ids = []
        self.db_ids = self.get_ids()
        self.db_queue = Queue()
        self.db_thread = Thread(target=self.save_to_database,
                                name='db_thread')
        self.api_queue = Queue()
        self.api_semaphore = Semaphore()
        self.api_threads = [Thread(target=self.collect_info,
                                   args=(self.config['api']['keys'][i % len(self.config['api']['keys'])],),
                                   name='api_thread_{}'.format(i)) for i in range(self.config['api']['threads'])]

        self.now = (datetime.now() + timedelta(hours=self.config['server']['timezoneDifference'])).date()
        self.limit = timedelta(self.config['api']['videos']['dateLimit'])

        if from_file:
            with open(self.config['files']['listFile'], 'r', encoding=self.config['files']['encoding']) as f:
                reader = DictReader(f)
                headers = reader.fieldnames
                for row in reader:
                    self.api_queue.put(row['channel_id'])
        else:
            for c in self.channel_ids:
                self.api_queue.put(c)

        self.db_thread.start()
        for t in self.api_threads:
            t.start()
        self.db_thread.join()
        for t in self.api_threads:
            t.join()
        self.logger.info('Finished execution.')
        with open(self.config['files']['collectIdFile'], 'w') as f:
            f.write(str(self.collect_id + 1))

    def create_log(self):
        logger = logging.getLogger('Monitor')
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler('{}/monitor_{}.log'.format(self.config['files']['logDirectory'],
                                                            datetime.now().strftime('%Y-%m-%d_%H-%M-%S')))
        fh.setFormatter(logging.Formatter(
            fmt='%(asctime)s - %(process)d - %(threadName)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
        return logger

    def get_ids(self):
        db = Database()
        results = {}
        try:
            channel_ids = db.select('channel', *['channel_id', 'yt_id'])
            self.channel_ids = [c['yt_id'] for c in channel_ids]
            for c in channel_ids:
                results[c['yt_id']] = c['channel_id']

            video_ids = db.select('video', *['video_id', 'yt_id'])
            for v in video_ids:
                results[v['yt_id']] = v['video_id']
        except Exception as err:
            self.logger.error('Failed to retrieve database IDs. {}'.format(repr(err)))
        return results

    def save_to_database(self):
        db = Database()
        query = None
        buffer = []
        empty = False
        while not empty:
            try:
                query = self.db_queue.get(timeout=240)
            except Empty:
                self.logger.warning('Database queue empty.')
                empty = True
            buffer.append(query)
            if len(buffer) >= self.config['database']['bufferLimit'] or (empty and len(buffer) > 0):
                buffer.sort(key=lambda k: k['table'])
                for key, group in groupby(buffer, key=lambda k: k['table']):
                    db_attempts = 3
                    while db_attempts:
                        try:
                            db.insert(key, [g['columns'] for g in group])
                            break
                        except Exception as err:
                            self.logger.error(
                                'Failed to save to database: {} Attempting {} more times.'.format(repr(err),
                                                                                                  db_attempts))
                            db_attempts -= 1
                buffer.clear()

    def collect_channel(self, api, channel_id):
        request, response = api.list('channels', **{'part': 'statistics', 'id': channel_id})
        try:
            statistics = response['items'][0]['statistics']
            query = {'table': 'collect_channel',
                     'columns': {'collect_id': self.collect_id, 'channel_id': self.db_ids[channel_id]}}
            query['columns']['subscriber_count'] = statistics['subscriberCount']
            query['columns']['collected_at'] = (datetime.now() + timedelta(
                hours=self.config['server']['timezoneDifference'])).strftime('%Y-%m-%d %H:%M:%S')
            self.db_queue.put(query)
        except KeyError as err:
            raise err

    def collect_videos(self, api, videos):
        for r in self.chunks(videos):
            request, response = api.list('videos', **{'part': 'statistics', 'id': ','.join(r)})
            try:
                for v in response['items']:
                    video_id = v['id']
                    statistics = v['statistics']
                    query = {'table': 'collect_video',
                             'columns': {'collect_id': self.collect_id, 'video_id': self.db_ids[video_id]}}
                    query['columns']['like_count'] = statistics.get('likeCount', 0)
                    query['columns']['dislike_count'] = statistics.get('dislikeCount', 0)
                    query['columns']['view_count'] = statistics.get('viewCount', 0)
                    query['columns']['comment_count'] = statistics.get('commentCount', 0)
                    query['columns']['collected_at'] = (datetime.now() + timedelta(
                        hours=self.config['server']['timezoneDifference'])).strftime('%Y-%m-%d %H:%M:%S')
                    self.db_queue.put(query)
            except KeyError as err:
                raise err

    def collect_info(self, key):
        api = APIRequest(api_key=key)
        db = Database()

        queue_attempts = 3

        # Gets channel from queue
        while queue_attempts:
            try:
                self.api_semaphore.acquire(blocking=True, timeout=120)
                channel_id = self.api_queue.get(timeout=120)
                print('{} - Thread {}'.format(self.api_queue.qsize(), current_thread().name))
            except Empty:
                queue_attempts -= 1
                self.logger.warning('API queue empty. Attempting {} more time(s).'.format(queue_attempts))
                continue
            finally:
                self.api_semaphore.release()

            # Checks if channel is already in database, otherwise saves it
            channel_dbid = self.db_ids.get(channel_id, 0)
            if not channel_dbid:
                try:
                    db_id_query = db.select('channel', *['channel_id'], where=['yt_id LIKE "{}"'.format(channel_id)])

                    if not db_id_query:
                        request, response = api.list('channels', **{'part': 'snippet', 'id': channel_id})
                        try:
                            snippet = response['items'][0]['snippet']
                            channel_query = {'yt_id': channel_id, 'title': snippet['title'],
                                             'description': snippet['description'],
                                             'published_at': self.parse_date(snippet['publishedAt'],
                                                                             return_datetime=True).strftime(
                                                 '%Y-%m-%d %H:%M:%S')}
                            channel_dbid = db.insert('channel', channel_query)
                            self.db_ids[channel_id] = channel_dbid
                        except KeyError as err:
                            self.logger.error('KeyError while getting channel info: {}'.format(repr(err)))
                            continue
                        except Exception as err:
                            self.logger.error('Error while getting channel info: {}'.format(repr(err)))
                            continue
                    else:
                        channel_dbid = db_id_query[0]['channel_id']
                        self.db_ids[channel_id] = channel_dbid
                except Exception as err:
                    self.logger.error(repr(err))
                    continue

            # Collects channel, attempting 3 times
            collect_attempts = 3
            while collect_attempts:
                try:
                    self.collect_channel(api, channel_id)
                    break
                except Exception as err:
                    collect_attempts -= 1
                    self.logger.error(
                        'Failed to collect channel {}: {} Attempting {} more times.'.format(channel_id, repr(err),
                                                                                            collect_attempts))
            if not collect_attempts:
                self.logger.info('Could not collect channel: {}'.format(channel_id))
                continue

            # Retrieves recent videos
            playlist_id = 'UU' + channel_id[2:]  # Playlist ID of channel c's uploads, can be derived from channel ID
            fetch_attempts = 3
            request, response = None, None
            while fetch_attempts:
                try:
                    request, response = api.list('playlistItems',
                                                 **{'part': 'contentDetails', 'playlistId': playlist_id,
                                                    'maxResults': 50})
                    break
                except Exception as err:
                    fetch_attempts -= 1
                    self.logger.error(
                        'Error while getting videos for channel {}: {} Attempting {} more times.'.format(channel_id,
                                                                                                         repr(err),
                                                                                                         fetch_attempts))
            if not fetch_attempts:
                self.logger.error('Failed to get videos for {}'.format(channel_id))
                continue
            video_list = []
            limit_reached = False
            while request and not limit_reached:
                for v in response['items']:
                    video_id = v['contentDetails']['videoId']
                    published_at = self.parse_date(v['contentDetails']['videoPublishedAt'])
                    if self.now - published_at > self.limit:
                        limit_reached = True
                        break
                    try:
                        db_id_query = \
                            db.select('video', 'video_id', where=['yt_id LIKE "{}"'.format(video_id)])
                        if not db_id_query:
                            request, response = api.list('videos', **{'part': 'snippet,contentDetails', 'id': video_id})
                            try:
                                snippet = response['items'][0]['snippet']
                                content_details = response['items'][0]['contentDetails']
                                video_query = {'yt_id': video_id, 'title': snippet['title'],
                                               'description': snippet['description'], 'channel_id': channel_dbid,
                                               'length_seconds': int(
                                                   parse_duration(content_details['duration']).total_seconds()),
                                               'published_at': self.parse_date(snippet['publishedAt'],
                                                                               return_datetime=True).strftime(
                                                   '%Y-%m-%d %H:%M:%S')}
                                video_dbid = db.insert('video', video_query)
                                self.db_ids[video_id] = video_dbid
                            except KeyError as err:
                                self.logger.error('KeyError while getting video info: {}'.format(repr(err)))
                                continue
                        else:
                            video_dbid = db_id_query[0]['video_id']
                            self.db_ids[video_id] = video_dbid
                    except Exception as err:
                        self.logger.error('Error while getting video data: {}'.format(repr(err)))
                        continue
                    video_list.append(video_id)
                collect_attempts = 3
                while collect_attempts:
                    try:
                        self.collect_videos(api, video_list)
                        break
                    except Exception as err:
                        collect_attempts -= 1
                        self.logger.error(
                            'Failed to collect videos from channel {}: '
                            '{} Attempting {} more times.'.format(channel_id,
                                                                  repr(err),
                                                                  collect_attempts))
                    finally:
                        video_list.clear()
                if not collect_attempts:
                    self.logger.info('Could not collect videos from channel: {}'.format(channel_id))

                if not limit_reached:
                    execute_attempts = 3
                    while execute_attempts:
                        try:
                            request, response = api.list_next('playlistItems', request, response)
                            break
                        except Exception as err:
                            execute_attempts -= 1
                            self.logger.error(
                                'Video fetch response error: {} Attempting {} more time(s)'.format(' '.join(err.args),
                                                                                                   execute_attempts))
                            continue
                    if not execute_attempts:
                        break
            queue_attempts = 3
        self.logger.info('Finished execution for Thread {}'.format(get_ident()))


Monitor()
