import pymongo.collection
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from DbAdapterClass import MongoAdapter
import yake
import os


def keyword_extraction(extract_tbl: pymongo.collection.Collection, text_field: str):
    # find items in extract_tbl which does not have a keywords field
    kwd_less = extract_tbl.find({text_field: {"$exists": True}})
    for document in kwd_less:
        keywords = kwe.extract_keywords(document[text_field])
        extract_tbl.updata_one(document, {'$set': {'keywords': keywords}})


if __name__ == '__main__':
    # read environmental vars
    DB_SERVER_URL = os.environ['DB_SERVER_URL']
    # initialise classes
    db = MongoAdapter(DB_SERVER_URL)
    kwe = yake.KeywordExtractor()
