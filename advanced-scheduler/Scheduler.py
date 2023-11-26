from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import yake
import pymongo.collection
import pickle
import re


class Scheduler:
    def __init__(self):
        # load keyword extractor and sentiment analyser
        self.kwe = yake.KeywordExtractor()
        self.sentiment = SentimentIntensityAnalyzer()
        # initialise location info dict
        self.loc_dict = {}
        # faster when re-using pre-compiled regex
        self.stripper = re.compile('[$&+,:;=?@#|\'\"~<>.^*()%!-]')

    def load_locations(self, pickle_path: str):
        # load locations dict using pickle file - faster, and require a smaller file
        # always maintain the data offine as json as pickle is version dependent
        with open(pickle_path, 'rb') as pkl_file:
            self.loc_dict = pickle.load(pkl_file)
        print('[INFO] loaded location records: ' + str(len(self.loc_dict)))

    def loc_extract(self, input_text: str) -> dict:
        # convert to lowercase (if applicable), split by all default separators
        # replace special char with space
        # lc_input = re.sub('[$&+,:;=?@#|\'\"~<>.^*()%!-]', ' ', input_text.lower())
        # we are using pre-compiled regex to improve perf
        lc_input = self.stripper.sub(' ', input_text.lower())
        # TODO: do we need to remove numbers
        word_list = lc_input.split()
        found_dict = {}
        for word in word_list:
            # we will use 'key in dict' for verification
            # this is the best method when vast number of missed hits
            # see https://stackoverflow.com/questions/28859095/most-efficient-method-to-check-if-dictionary-key-exists-and-process-its-value-if
            if word in self.loc_dict:
                if word in found_dict:
                    count = found_dict[word] + 1
                else:
                    count = 1
                found_dict[word] = count
        return found_dict

    def text_extraction(self, extract_tbl: pymongo.collection.Collection,
                        text_fields: list[str], schedule_type: str) -> dict:
        # find items in extract_tbl which does not have schedule_type (same as info) field defined
        unextracted = extract_tbl.find({schedule_type: {"$exists": False}})
        count = 0
        for document in unextracted:
            count += 1
            # WARN: extracted_data field represents any kind of extracted data
            # could be a list, value or a dict
            extracted_data = None
            # text string is created using string join space separator
            text_values = []
            for i in text_fields:
                text_values.append(document[i])
            text_string = ' '.join(text_values)
            match schedule_type:
                case 'keyword_extraction':
                    extracted_data = self.kwe.extract_keywords(text_string)
                case 'location_extraction':
                    extracted_data = self.loc_extract(text_string)
                case 'sentiment_analysis':
                    extracted_data = self.sentiment.polarity_scores(text_string)
            extract_tbl.update_one(document, {'$set': {schedule_type: extracted_data}})
        report = {'modified_documents': count}
        return report


