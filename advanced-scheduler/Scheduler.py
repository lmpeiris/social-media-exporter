from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import yake
import pymongo.collection


class Scheduler:
    def __init__(self):
        self.kwe = yake.KeywordExtractor()

    def keyword_extraction(self, extract_tbl: pymongo.collection.Collection, text_field: str) -> dict:
        # find items in extract_tbl which does not have a keywords field
        kwd_less = extract_tbl.find({text_field: {"$exists": False}})
        count = 0
        for document in kwd_less:
            count += 1
            keywords = self.kwe.extract_keywords(document[text_field])
            extract_tbl.update_one(document, {'$set': {'keywords': keywords}})
        report = {'modified_documents': count}
        return report
