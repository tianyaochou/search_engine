from email import message
from nltk.corpus.reader.wordnet import WordNetError
from nltk.stem.snowball import EnglishStemmer
from nltk.tokenize import word_tokenize
from nltk import corpus
import redis
import os
import email

from redis.utils import pipeline

MAIL_DIR = "maildir"

r = redis.Redis("localhost", port=6379, db=0)
stemmer = EnglishStemmer()
stopwords = corpus.stopwords.words("english")
idx = {}


def tokenize_email(entry):
    """
    tokenize email, returns list of tokens
    """
    tokens = set()
    with open(entry, encoding="ascii", errors="surrogateescape") as f:
        message = email.message_from_file(f)
        f.close()
        for token in filter(  # filter out stopwords and single-character token
            lambda token: len(token) > 1 and (token not in stopwords),
            map(  # stemming all tokens
                stemmer.stem,
                word_tokenize(message.get("Subject") + "\n" + message.get_payload()),
            ),
        ):
            tokens.add(token)
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
        r.set("file:{}.id".format(filepath), fileid)
        r.set("file:{}.path".format(fileid), filepath)
        return int(fileid)


def reverse_index_email(entry):
    """
    reverse index a email
    """
    fileid = getfileid(os.path.relpath(entry.path, start=MAIL_DIR))
    if fileid < int(r.get("fileid")) - 1:
        return
    pipeline = r.pipeline(True)
    for token in tokenize_email(entry):
        tokenid = gettokenid(token)
        # idx[tokenid] = fileid
        pipeline.sadd("idx:{}".format(tokenid), fileid)
    return len(pipeline.execute())


def process_files(path: str):
    """
    recursively iterate over files in given path
    """
    ignore_files = {}
    entry_stack: set[os.DirEntry] = set()
    for entry in os.scandir(path):
        entry_stack.add(entry)
    count = 0
    while len(entry_stack) != 0:
        entry = entry_stack.pop()
        if entry.is_dir():
            for subentry in os.scandir(entry):
                entry_stack.add(subentry)
        elif entry.name[0] != "." and entry.is_file():
            reverse_index_email(entry)
            count += 1
            print("\r{}".format(count), end="")


if __name__ == "__main__":
    process_files(MAIL_DIR)