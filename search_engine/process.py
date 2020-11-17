from email import message
from operator import index
from nltk.stem.snowball import EnglishStemmer
from nltk.tokenize import word_tokenize
from nltk import corpus
import redis
import os
import email
import pqdict

from redis.utils import pipeline

MAIL_DIR = "maildir"
files = {}
tfs = {}
token_counts = pqdict.maxpq()
tokens_1000 = set()
files_count = 0

r = redis.Redis("localhost", port=6379, db=0)
stemmer = EnglishStemmer()
stopwords = corpus.stopwords.words("english")


def tokenize_email(fullemail: str):
    """
    tokenize email, returns list of tokens
    """
    tokens = []
    message = email.message_from_string(fullemail)
    for token in filter(  # filter out stopwords and single-character token
        lambda token: len(token) > 1 and (token not in stopwords),
        map(  # stemming all tokens
            stemmer.stem,
            word_tokenize(message.get("Subject") + "\n" + message.get_payload()),
        ),
    ):
        tokens.append(token)
    return tokens


def gettokenid(token: str):
    """
    get id for a token, if it has no id, generate one
    """
    id = r.get("token:{}".format(token))
    if id:
        return int(id)
    else:
        tokenid = r.incr("tokenid")
        r.set("token:{}".format(token), tokenid)
        return int(tokenid)


def getfileid(filepath):
    """
    get id for a file, if it has no id, generate one
    """
    id = r.get("file:{}".format(filepath))
    if id:
        return int(id)
    else:
        fileid = r.incr("fileid")
        r.set("file:{}".format(filepath), fileid)
        r.set("file:{}".format(fileid), filepath)
        return int(fileid)


def index_file(path, tokens):
    """
    reverse index files and store it in redis
    """
    fileid = getfileid(path)
    if fileid < int(r.get("fileid")) - 1:
        return
    pipeline = r.pipeline(True)
    for token in tokens:
        tokenid = gettokenid(token)
        if token in tokens_1000:
            pipeline.sadd("idx:{}".format(tokenid), fileid)
            pipeline.set("tf:{}:{}".format(fileid, tokenid), tfs[(path, token)])
            pipeline.incr("df:{}".format(tokenid))
    return len(pipeline.execute())


def load_files(path: str):
    """
    recursively iterate over files in given path
    """

    def norm_path(path):
        return os.path.relpath(path, start=MAIL_DIR).replace("\\", "/")

    print("Loading files...")
    ignore_files = {}
    count = 0
    entry_stack: list[os.DirEntry] = []
    for entry in os.scandir(path):
        entry_stack.append(entry)
    while len(entry_stack) != 0:
        entry = entry_stack.pop()
        if entry.is_dir():
            for subentry in os.scandir(entry):
                entry_stack.append(subentry)
        elif entry.name[0] != "." and entry.is_file():
            f = open(entry, encoding="ascii", errors="surrogateescape")
            files[norm_path(entry.path)] = f.read()
            f.close()
            count += 1
            print("\r{}".format(count), end="")
    files_count = count


def count_tokens():
    print("\nCounting tokens")
    count = 0
    for path, content in files.items():
        tokens = tokenize_email(content)
        for token in tokens:
            if token_counts.get(token):
                token_counts[token] += 1
            else:
                token_counts[token] = 1
            if tfs.get((path, token)):
                tfs[(path, token)] += 1
            else:
                tfs[(path, token)] = 1

        count += 1
        print("\r{}/{}".format(count, files_count), end="")
        files[path] = set(tokens)


def get_1000_tokens():
    for i in range(0, 1000):
        tokens_1000.add(token_counts.pop())


def reverse_index_files():
    print("\nIndexing files...")
    count = 0
    for path, tokens in files.items():
        index_file(path, tokens)
        count += 1
        print("\r{}/{}".format(count, files_count), end="")


if __name__ == "__main__":
    load_files(MAIL_DIR)
    count_tokens()
    get_1000_tokens()
    reverse_index_files()