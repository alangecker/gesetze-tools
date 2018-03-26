"""LawDe.

Usage:
  lawde.py load [--path=<path>] <law>...
  lawde.py loadall [--path=<path>]
  lawde.py updatelist
  lawde.py -h | --help
  lawde.py --version

Examples:
  lawde.py load kaeaano

Options:
  --path=<path>  Path to laws dir [default: laws].
  -h --help     Show this screen.
  --version     Show version.

Duration Estimates:
  2-4 hours for total download

"""
import datetime
import os
import re
from StringIO import StringIO
import json
import shutil
import time

from docopt import docopt
import requests
import zipfile
from xml.dom.minidom import parseString
from Queue import Queue
from threading import Thread

class Lawde(object):
    BASE_URL = 'http://www.gesetze-im-internet.de'
    BASE_PATH = 'laws/'
    INDENT_CHAR = ' '
    INDENT = 2

    def __init__(self, path=BASE_PATH, lawlist='data/laws.json',
                **kwargs):
        self.indent = self.INDENT_CHAR * self.INDENT
        self.path = path
        self.lawlist = lawlist

    def build_zip_url(self, law):
        url = '%s/%s/xml.zip' % (self.BASE_URL, law)
        return url

    def download_law(self, law):
        tries = 0
        while True:
            try:
                res = requests.get(self.build_zip_url(law))
                # file('test.zip', 'w').write(res.content)
            except Exception as e:
                tries += 1
                print e
                if tries > 3:
                    raise e
                else:
                    print "Sleeping %d" % tries * 3
                    time.sleep(tries * 3)
            else:
                break
        try:
            zipf = zipfile.ZipFile(StringIO(res.content))
        except zipfile.BadZipfile:
            print "Removed %s" % law
            self.remove_law(law)
            return None
        return zipf

    def load(self, laws):
        queue = Queue()
        total = float(len(laws))
        ts1 = datetime.datetime.now()
        print "Laws to download: %d" % len(laws)

        def workerThread():
            while not queue.empty():
                law,i = queue.get()
                if i == 39:
                    ts2 = datetime.datetime.now()
                    ts_diff = ts2 - ts1
                    print "Estimated download time: %d minutes" % ((ts_diff.seconds * len(laws)/39) / 60)
                if i % 10 == 0:
                    print '%.1f%%' % (i / total * 100)
                zipfile = self.download_law(law)
                if zipfile is not None:
                    self.store(law, zipfile)
                queue.task_done()

        for i, law in enumerate(laws):
            queue.put((law,i))

        for x in range(10):
            worker = Thread(target=workerThread)
            worker.daemon = True
            worker.start()


        while not queue.empty():
            time.sleep(5)


    def build_law_path(self, law):
        prefix = law[0]
        return os.path.join(self.path, prefix, law)

    def remove_law(self, law):
        law_path = self.build_law_path(law)
        shutil.rmtree(law_path, ignore_errors=True)

    def store(self, law, zipf):
        self.remove_law(law)
        law_path = self.build_law_path(law)
        norm_date_re = re.compile('<norm builddate="\d+"')
        os.makedirs(law_path)
        for name in zipf.namelist():
            if name.endswith('.xml'):
                xml = zipf.open(name).read()
                xml = norm_date_re.sub('<norm', xml)
                dom = parseString(xml)
                xml = dom.toprettyxml(encoding='utf-8',
                    indent=self.indent)
                if not name.startswith('_'):
                    law_filename = os.path.join(law_path, '%s.xml' % law)
                else:
                    law_filename = name
                file(law_filename, 'w').write(xml)
            else:
                zipf.extract(name, law_path)

    def get_all_laws(self):
        return [l['slug'] for l in json.load(file(self.lawlist))]

    def loadall(self):
        self.load(self.get_all_laws())

    def update_list(self):
        BASE_URL = 'http://www.gesetze-im-internet.de/Teilliste_%s.html'
        CHARS = 'abcdefghijklmnopqrstuvwxyz0123456789'
        # Evil parsing of HTML with regex'
        REGEX = re.compile('href="\./([^\/]+)/index.html"><abbr title="([^"]*)">([^<]+)</abbr>')

        laws = []

        for char in CHARS:
            print "Loading part list %s" % char
            try:
                response = requests.get(BASE_URL % char.upper())
                html = response.content
            except Exception:
                continue
            html = html.decode('iso-8859-1')
            matches = REGEX.findall(html)
            for match in matches:
                laws.append({
                    'slug': match[0],
                    'name': match[1].replace('&quot;', '"'),
                    'abbreviation': match[2].strip()
                })
        json.dump(laws, file(self.lawlist, 'w'))


def main(arguments):
    nice_arguments = {}
    for k in arguments:
        if k.startswith('--'):
            nice_arguments[k[2:]] = arguments[k]
        else:
            nice_arguments[k] = arguments[k]
    lawde = Lawde(**nice_arguments)
    if arguments['load']:
        lawde.load(arguments['<law>'])
    elif arguments['loadall']:
        lawde.loadall()
    elif arguments['updatelist']:
        lawde.update_list()

if __name__ == '__main__':
    arguments = docopt(__doc__, version='LawDe 0.0.1')
    main(arguments)
