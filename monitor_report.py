from configurable import Configurable
from tools import Database, APIRequest
import pymysql
from pymysql import cursors
from csv import DictWriter, QUOTE_MINIMAL
from datetime import datetime
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, letter, landscape, A3
from reportlab.lib.units import inch, cm
from sys import argv


class PDFManager(Configurable):

    def __init__(self):
        super().__init__(self)

    def save_pdf(self, filename, tables_data, header, size=landscape(A3)):
        pdf = SimpleDocTemplate('reports/{}.pdf'.format(filename), pagesize=size)
        pdf_elements = []
        for t in tables_data:
            data = t
            data.insert(0, header)
            table = Table(t)
            table.setStyle(TableStyle([
                ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
                ('BOX', (0, 0), (-1, -1), 1, colors.black)
            ]))
            pdf_elements.append(table)
            pdf_elements.append(Spacer(1, 0.25 * cm))
        pdf.build(pdf_elements)


class Report(Configurable):

    def __init__(self, days=7, limit_per_table=20, save_pdf=False):
        super().__init__(self)
        self.days = int(days)
        self.data = self.get_data()
        self.get_views(self.data)
        self.results = sorted(self.data, key = lambda i: i['view_count'], reverse=True)
        fields = ['video_title', 'video_yt_id', 'view_count' , 'channel_name', 'channel_yt_id', 'published_at', 'channel_cluster']
        filename = 'report_{}'.format(datetime.now().date())
        with open('reports/{}.csv'.format(filename), 'w', encoding='utf8') as f:
            w = DictWriter(f, delimiter=',', quotechar='"', quoting= QUOTE_MINIMAL, fieldnames=fields, lineterminator='\n', extrasaction='ignore')
            w.writeheader()
            w.writerows(self.results)

        if save_pdf:
            fields.remove('channel_cluster')
            fields.remove('channel_yt_id')
            self.pdf = PDFManager()
            clusters = {}
            data_per_cluster = {}
            for row in self.results:
                if row['channel_cluster'] is None:
                    continue
                current_cluster = clusters.get(row['channel_cluster'],0)
                if current_cluster < limit_per_table:
                    clusters[row['channel_cluster']] = clusters.get(row['channel_cluster'], 0) + 1
                    data_per_cluster[row['channel_cluster']] = data_per_cluster.get(row['channel_cluster'], [])
                    pdf_row = {k: row[k] for k in fields if k in row}
                    data_per_cluster[row['channel_cluster']].append([v for k, v in pdf_row.items()])
            self.pdf.save_pdf(filename, [v for k, v in data_per_cluster.items()], fields)



    def get_views(self, videos):
        d = Database()
        with open('.COLLECT') as f:
            collect_id = int(f.readline().strip())
        views = d.select('collect_video', *['video_id','MAX(view_count) as view_count'],where=['COLLECT_id > {} GROUP BY video_id'.format(collect_id - self.days - 10)])
        for v in videos:
            try:
                v['view_count'] = next(item for item in views if item['video_id'] == v['video_id'])['view_count']
            except StopIteration as e:
                print('Error with video {}:\n{}\n\n'.format(v, repr(e)))
                v['view_count'] = 0

    def get_data(self):
        results = []
        conn = pymysql.connect(**self.config['database']['connection'])
        try:
            with conn.cursor(cursors.DictCursor) as cursor:
                sql = 'SELECT v.video_id, v.yt_id as video_yt_id, v.title as video_title, v.published_at as published_at, c.yt_id as channel_yt_id, c.title as channel_name, c.cluster as channel_cluster from video v JOIN channel c ON v.channel_id = c.channel_id AND v.published_at >= DATE(NOW()) - INTERVAL {} DAY'.format(self.days)
                cursor.execute(sql)
                results = cursor.fetchall()
        except pymysql.MySQLError as err:
            print(err)
        finally:
            conn.close()
            return results


days = int(argv[1]) if len(argv) > 1 else 7
Report(days=days)
