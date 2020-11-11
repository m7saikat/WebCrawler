import re
import time
import urllib
from urllib.parse import urljoin
from urllib.parse import urlparse
import urllib.robotparser
import tldextract


import urllib3
from bs4 import     BeautifulSoup as bs
from queue import PriorityQueue
import requests
from reppy.robots import Robots

from constants import *
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
# import w3lib.url
from   tqdm import tqdm
import dill
from pathlib import Path
import hashlib

parser = urllib.robotparser.RobotFileParser()
ps = PorterStemmer()

class LinkStore:
    def __init__(self, link, wave_num, score_priority, in_links=None, out_links=None):
        self.link = link
        self.wave_num = wave_num
        self.score_priority = score_priority

    def __eq__(self, obj):
        return obj.link == self.link \
               and obj.score_priority == self.score_priority \
               and obj.wave_num == self.wave_num
               # and obj.in_links == self.in_links \
               # and obj.out_links == self.out_links \

    def __lt__(self, other):
        return sum(self.score_priority) < sum(other.score_priority)

    def __gt__(self, other):
        return sum(self.score_priority) >= sum(other.score_priority)


class Crawl:

    doc_count = 0
    inlinks = {}
    oulinks = {}
    visited = set()
    robot_dict = {}
    traversed = []
    doc_written = set()

    def __init__(self, frontier=None, aux_ftier=None):
        self.frontier = frontier
        self.aux_ftier = aux_ftier
        self.file_no = 0

    @staticmethod
    def get_file_names(file_no):
        PATH_TO_DOC_COUNT = ROOT / 'doc_count_{}'.format(file_no)
        PATH_TO_INLINKS = ROOT / 'INLINKS_{}'.format(file_no)
        PATH_TO_OUTLINKS = ROOT / 'OUTLINKS_{}'.format(file_no)
        PATH_TO_TRAVERSED = ROOT / 'traversed_{}'.format(file_no)
        PATH_TO_VISITED = ROOT / 'visited_{}'.format(file_no)
        PATH_TO_ROBOT_DIC = ROOT / 'robot_dic_{}'.format(file_no)
        PATH_TO_FRONTIER = ROOT / 'frontier_{}'.format(file_no)
        PATH_TO_AUX_FRONTIER = ROOT / 'aux_frontier_{}'.format(file_no)
        PATH_TO_DOC_WRITTEN = ROOT / 'doc_written_{}'.format(file_no)

        return {
            'PATH_TO_DOC_COUNT': PATH_TO_DOC_COUNT,
            'PATH_TO_INLINKS': PATH_TO_INLINKS,
            'PATH_TO_OUTLINKS': PATH_TO_OUTLINKS,
            'PATH_TO_TRAVERSED': PATH_TO_TRAVERSED,
            'PATH_TO_VISITED': PATH_TO_VISITED,
            'PATH_TO_ROBOT_DIC': PATH_TO_ROBOT_DIC,
            'PATH_TO_FRONTIER': PATH_TO_FRONTIER,
            'PATH_TO_AUX_FRONTIER': PATH_TO_AUX_FRONTIER,
            'PATH_TO_DOC_WRITTEN': PATH_TO_DOC_WRITTEN
        }

    def write_state(self, file_no):
        self.file_no = file_no
        files = self.get_file_names(self.file_no)

        with open((files['PATH_TO_DOC_WRITTEN']), 'wb') as f:
            dill.dump(self.doc_written, f)

        # PATH_TO_DOC_COUNT = ROOT /'doc_count_{}'.format(file_no)
        dill.dump(self.doc_count, open(files['PATH_TO_DOC_COUNT'], 'wb'))

        # PATH_TO_INLINKS = ROOT /'INLINKS_{}'.format(file_no)
        dill.dump(self.inlinks, open(files['PATH_TO_INLINKS'], 'wb'))

        # PATH_TO_OUTLINKS = ROOT /'OUTLINKS_{}'.format(file_no)
        dill.dump(self.oulinks, open(files['PATH_TO_OUTLINKS'], 'wb'))

        # PATH_TO_TRAVERSED =  ROOT /'traversed_{}'.format(file_no)
        dill.dump(self.traversed, open(files['PATH_TO_TRAVERSED'], 'wb'))

        # PATH_TO_VISITED = ROOT / 'visited_{}'.format(file_no)
        dill.dump(self.visited, open(files['PATH_TO_VISITED'], 'wb'))

        # PATH_TO_ROBOT_DIC = ROOT / 'robot_dic_{}'.format(file_no)
        dill.dump(self.robot_dict, open(files['PATH_TO_ROBOT_DIC'], 'wb'))

        # PATH_TO_FRONTIER = ROOT / 'frontier_{}'.format(file_no)
        dill.dump(self.frontier, open(files['PATH_TO_FRONTIER'], 'wb'))

        # PATH_TO_AUX_FRONTIER = ROOT / 'aux_frontier_{}'.format(file_no)
        dill.dump(self.aux_ftier, open(files['PATH_TO_AUX_FRONTIER'], 'wb'))

        # PATH_TO_FILE_NUM = ROOT / 'file_num'
        dill.dump(file_no, open(PATH_TO_FILE_NUM, 'wb'))

    @staticmethod
    def load_file(path):
        with open(path, "rb") as file:
            return dill.load(file)

    def resume_crawl(self):
        self.file_no = self.load_file(PATH_TO_FILE_NUM)
        files = self.get_file_names(self.file_no)

        self.aux_ftier = self.load_file(files['PATH_TO_AUX_FRONTIER'])
        self.frontier = self.load_file(files['PATH_TO_FRONTIER'])
        self.robot_dict = self.load_file(files['PATH_TO_ROBOT_DIC'])
        self.visited = self.load_file(files['PATH_TO_VISITED'])
        self.traversed = self.load_file(files['PATH_TO_TRAVERSED'])
        self.oulinks = self.load_file(files['PATH_TO_OUTLINKS'])
        self.inlinks = self.load_file(files['PATH_TO_INLINKS'])
        self.doc_count = self.load_file(files['PATH_TO_DOC_COUNT'])
        self.doc_written = self.load_file(files['PATH_TO_DOC_WRITTEN'])

        self.crawl_seeds()

    @staticmethod
    def url_canonicalization(url, domain=None):
        url = w3lib.url.canonicalize_url(url)
        if url.endswith('.jpg') or url.endswith('.pdf') or url.endswith('.jpeg') or url.endswith('.png') or url.endswith('.webm'):
            return None
        if url is '/':
            return None
        if url.startswith("https://"):
            url = 'http' + url[5:]
        if not url.startswith("http"):
            url = urljoin(domain, url)
        if url.startswith("http") and url.endswith(":80"):
            url = url[:-3]
        if url.startswith("https") and url.endswith(":443"):
            url = url[:-4]
        url = url.rsplit('#', 1)[0]

        return url

    @staticmethod
    def remove_(text):
        return tldextract.extract(text).registered_domain

    @staticmethod
    def get_stemmed(processed_list):
        stemmed = []
        for word in processed_list:
            stemmed.append(ps.stem(word))
        return stemmed

    def get_body_score(self, body):
        stop_words = set(stopwords.words('english'))
        stemmed_related_word = set(self.get_stemmed(related_words))
        body_list = body.split()
        filtered_body =[w for w in body_list if w not in stop_words and len(body_list)]
        stemmed_body = [ps.stem(word) for word in filtered_body] if len(filtered_body) else []
        body_match = stemmed_related_word.intersection(stemmed_body)
        return len(body_match)

    def get_score(self, base_url, title, a_tag_text, link_text, inlink_score):
        stop_words = set(stopwords.words('english'))
        if title is None:
            title = ''

        filtered_a_tag_text = [w for w in a_tag_text.split() if w not in stop_words and len(a_tag_text.split())]
        filtered_link_text = [w for w in link_text if w not in stop_words and len(link_text)]
        filtered_title = [w for w in title.split() if w not in stop_words and len(title.split())]

        stemmed_related_word = set(self.get_stemmed(related_words))

        stemmed_a_tag_text = [ps.stem(word) for word in filtered_a_tag_text] if len(filtered_a_tag_text) else []
        stemmed_title = [ps.stem(word) for word in filtered_title] if len(filtered_title) else []
        stemmed_link_text = [ps.stem(word) for word in filtered_link_text] if len(filtered_link_text) else []

        link_text_match = stemmed_related_word.intersection(stemmed_link_text)
        title_match = stemmed_related_word.intersection(stemmed_title)
        a_tag_match = stemmed_related_word.intersection(stemmed_a_tag_text)

        url_score_set = set()
        url_score_set.update(link_text_match)
        url_score_set.update(title_match)
        url_score_set.update(a_tag_match)

        url_score = len(url_score_set)

        if base_url in DOMAIN_SCORE.keys():
           domainscore = DOMAIN_SCORE[base_url]
        else:
           domainscore = 1

        if not url_score_set:
            url_score = 0

        score = [url_score, domainscore, inlink_score]
        return score

    def is_crawl_allowed(self, url, base_url):
        # scheme, netloc_host, path, params, query, fragment = urlparse(url)
        print("In is_crawl_allowed")
        time_start = time.time()
        robot_url = base_url + 'robots.txt'
        try:
            # if robot_url not in self.robot_dict.keys():
            robots = Robots.fetch(robot_url, timeout=3)
            print("Robot permission for {}: {} ".format(robot_url, robots.allowed(url, '*')))
            # delay = robots.agent('*').delay
            # time.sleep(delay)
            self.robot_dict[robot_url] = robots.allowed(url, '*')
            print('time taken to crawl: {}'.format(time.time() - time_start))
            return robots.allowed(url, '*')
            # else:
            #     print('time taken to crawl: {}'.format(time.time() - time_start))
            #     return self.robot_dict[robot_url]
        except Exception as e:
            print("Exception is {}".format(e))
            self.robot_dict[robot_url] = False
            print('time taken to crawl: {}'.format(time.time() - time_start))
            return False

    def create_doc_index(self, url, text, title):
        # output = open(filename, "w")
        data = '<DOC>\n' \
               + '<DOCNO>' + url + '</DOCNO>\n' \
               + '<HEAD>' + title + '</HEAD>\n' \
               + '<TEXT>' + text + '</TEXT>\n' \
               + '</DOC>'
        # output.write(data)
        # output.close()
        return data

    @staticmethod
    def write_doc(data, filename):
        print("----------WRITING TO FILE-------------")
        with open(filename, "wb") as f:
            data = data.strip()
            try:
                f.write(data.encode('utf-8'))
                print('----------FILE WRITTEN------------')
            except Exception:
                print("*********FILE NOT WRITTEN**********")

    @staticmethod
    def parse_url(url):
        try:
            page = requests.get(url, headers=DUMMY_HEADER, timeout=3)
            page_as_text = page.text
        except requests.exceptions.Timeout:
            return
        except requests.exceptions.ConnectionError:
            return
        except requests.exceptions.ChunkedEncodingError:
            return
        except requests.exceptions.TooManyRedirects:
            return
        except requests.exceptions.InvalidSchema:
            return
        except requests.exceptions.ContentDecodingError:
            return
        except UnicodeError:
            return

        soup = bs(page_as_text, 'html.parser')
        title = soup.title.string if soup.title else ''
        # header = page.headers
        if 'web.archive.org' in url:
            body = [''.join(s.findAll(text=True)) for s in soup.findAll(class_='fbody')]
            body_p = [''.join(s.findAll(text=True)) for s in soup.findAll('p')]
            if len(body_p):
                body += body_p
        else:
            body = [''.join(s.findAll(text=True)) for s in soup.findAll('p')]
        body = ' '.join(body) if len(body) else ''
        return title, body.strip()

    def crawl_page(self, url_obj, base_url, delay=False):
        url = url_obj.link
        try:
            response = requests.get(url, headers=DUMMY_HEADER, timeout=3)
            page = response.text

        except requests.exceptions.Timeout:
            return
        except requests.exceptions.ConnectionError:
            return
        except requests.exceptions.ChunkedEncodingError:
            return
        except requests.exceptions.TooManyRedirects:
            return

        time_start = time.time()
        time.sleep(1) if delay else None
        soup = bs(page, 'html.parser')
        a_tags = soup.find_all('a')
        for link_tag in tqdm(a_tags):
            link = link_tag.get('href')
            if link is None:
                continue
            link_text = self.get_link_text(link)
            link = self.url_canonicalization(link, base_url)
            if link is None:
                continue
            title = link_tag.get('title')
            a_tag_text = link_tag.text
            if link:
                if link in self.inlinks.keys():
                    self.inlinks[link].add(url)
                else:
                    self.inlinks[link] = set()
                    self.inlinks[link].add(url)

            score = self.get_score(base_url, title, a_tag_text, link_text, len(self.inlinks[link]))
            if not score[0]:
                continue

            if link:
                if url in self.oulinks.keys():
                    self.oulinks[url].add(link)
                else:
                    self.oulinks[url] = {link}

            wave_num = url_obj.wave_num + 1
            if wave_num == 5:
                a = ''
            li_obj = LinkStore(link, wave_num, score)
            self.aux_ftier.put((score, li_obj)) if link not in self.traversed and self.aux_ftier.qsize() <= 60000 else None
            self.traversed.append(link)
        time_end = time.time() - time_start
        print("time to crawl: {}\n\n".format(time_end))

    def crawl_seeds(self):
        current_domain = ''
        file_num_count = self.file_no + 1
        doc_text = ''

        while self.doc_count < COUNT_DOC: # or not self.frontier.empty():
            delay = False

            if self.frontier.empty():
                self.frontier = self.aux_ftier
                self.aux_ftier = ReversePriorityQueue()

            score, url_obj = self.frontier.get()
            url = url_obj.link
            parsed_uri = urlparse(url)
            domain = parsed_uri.netloc
            base_url = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)

            print("Getting links for url: {}".format(url))
            print("Base URL : " + base_url)

            if url.endswith('.jpg') \
                    or url.endswith('.pdf') \
                    or url.endswith('.jpeg') \
                    or url.endswith('.png') \
                    or url.endswith('.webm') \
                    or url.endswith('.wmv') \
                    or url.endswith('.ogv')\
                    or url.startswith(blocked_url):
                continue

            if base_url in blacklist_domains:
                continue
            if current_domain == domain:
                delay = True
            current_domain = parsed_uri.netloc
            time_start = time.time()
            robot_url = base_url + 'robots.txt'
            if robot_url in self.robot_dict.keys() or self.is_crawl_allowed(url, base_url):

                try:
                    title, body = self.parse_url(url)  # increase doc_counter
                except TypeError:
                    a = ''
                    # self.doc_count -= 1
                    continue

                if title:
                    title = title.strip()
                else:
                    title = "no title"

                if self.get_body_score(body) and url not in self.doc_written:
                    doc_text = '{}\n{}'.format(doc_text, self.create_doc_index(url, body, title.strip()))
                    self.doc_written.add(url)
                else:
                    # self.doc_count -= 1
                    print('Document skipped - body: {}'.format(body))
                    continue

                self.crawl_page(url_obj, base_url, delay)
                self.doc_count += 1
                self.visited.add(url)

            else:
                continue
            print("Frontier Size: {}".format(self.frontier.qsize()))
            print("Aux Frontier Size: {}".format(self.aux_ftier.qsize()))
            print('Doc scanned: {}'.format(self.doc_count))
            print('Wave Num: {}'.format(url_obj.wave_num))

            if self.doc_count % 200 == 0:
                self.write_state(file_num_count)
                self.write_doc(doc_text, "{}\doc_{}".format(ROOT2, file_num_count))
                doc_text = ''
                file_num_count += 1

    def get_link_text(self, link):
        return [word.lower() for word in re.findall(r"[A-Za-z]+", link)]

    def get_domain_score(self, base_url):
        return DOMAIN_SCORE[base_url]
        pass


class ReversePriorityQueue(PriorityQueue):
    def put(self, tup):
        score_list = tup[0]
        new_score_list = [-1*score for score in score_list]
        newtup = new_score_list, tup[1]
        PriorityQueue.put(self, newtup)

    def get(self):
        tup = PriorityQueue.get(self)
        score_list = tup[0]
        new_score_list = [-1 * score for score in score_list]
        newtup = new_score_list, tup[1]
        return newtup

if __name__ == '__main__':
    ftier = ReversePriorityQueue()
    aux_ftier = ReversePriorityQueue()

    seed = ["http://en.wikipedia.org/wiki/List_of_terrorist_incidents",
            "http://en.wikipedia.org/wiki/Boston_Marathon_bombings",
            "https://www.google.com/search?rls=en&q=boston+marathon+bombing+motive&ie=UTF-8&oe=UTF-8",
            "https://en.wikipedia.org/wiki/2016_Orlando_nightclub_shooting",
            "https://en.wikipedia.org/wiki/2015_San_Bernardino_attack"
            ]

    for li in seed:
        li_obj = LinkStore(Crawl.url_canonicalization(li), 0,  [10000000])
        ftier.put(([10000000], li_obj))

    # crawler = Crawl(ftier, aux_ftier)
    #
    # crawler.crawl_seeds()

    crawler = Crawl()
    crawler.resume_crawl()
