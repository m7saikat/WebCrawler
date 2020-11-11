from pathlib import Path

related_words = ['terror', 'blast', 'bomb', 'explosives', 'isis', 'hostages', 'terrorist', 'attack', 'insurgent',
                 'threat', 'violence', 'bombing', 'militant', 'separatist', 'al qaeda', 'isis', 'shooting', 'militancy',
                 'Boko Haram', 'Harkat-ul-Mujahideen', 'Islamic State of Iraq and the Levant', '	Lashkar-e-Taiba',
                 'sucide bombing']

DUMMY_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

blacklist_domains = ['www.recordamerican.com',
                     'https://www.facebook.com/',
                     'https://tools.wmflabs.org/',
                     'http://www.standardmedia.co.ke/',
                     'https://ondemand.npr.org/',
                     'https://web.archive.org/',
                     'http://web.archive.org/',
                     'http://tools.wmflabs.org/',
                     'https://tools.wmflabs.org/']

blocked_url = 'http://en.wikipedia.org/w/index.php?action=edit'

DOMAIN_SCORE = {
    'http://en.wikipedia.org/': 5,
    'https://en.wikipedia.org/': 5,
    'https://www.nytimes.com/': 4,
    'http://www.nytimes.com/': 4,
    'https://www.washingtontimes.com/': 4,
    'http://www.washingtontimes.com/': 4,
    'http://www.thetimes.co.uk/': 4,
    'https://www.thetimes.co.uk/': 4,
    'https://www.reuters.com/': 4,
    'http://www.reuters.com/': 4,
    'http://www.cnn.com/': 4,
    'https://www.cnn.com/': 4
}

COUNT_DOC = 47000
doc_counter = 0

ROOT_exp = Path(__file__).parents[1]
PATH_TO_DOC_2 = ROOT_exp / 'DOCS2'
PATTERN_DOC = r'<DOC>.*?</DOC>'
PATTERN_TEXT = r'<TEXT>(.*?)</TEXT>'

PATH_TO_DOC = ROOT_exp / 'DOCS'

ROOT = Path(__file__).parents[1] / 'state/'
ROOT2 = "D:\\CS-6200\\Assignments\\Assignment3\\DOCS"
#
PATH_TO_FILE_NUM = ROOT / 'file_num'

PATTERN_HTTP = r'^http://.*?'
PATTERN_DOC = r'<DOC>.*?</DOC>'
PATTERN_DOC_NO = r'<DOCNO>(.*?)</DOCNO>'
PATTERN_TEXT = r'<TEXT>(.*?)</TEXT>'

# PATH_TO_INLINKS = ROOT / 'INLINKS'
# PATH_TO_OUTLINKS = ROOT / 'OUTLINKS'
# PATH_TO_TRAVERSED = ROOT / 'traversed'
# PATH_TO_VISITED = ROOT / 'visited'
# PATH_TO_ROBOT_DIC = ROOT / 'robot_dic'
# PATH_TO_FRONTIER = ROOT / 'frontier'
# PATH_TO_AUX_FRONTIER = ROOT / 'aux_frontier'
