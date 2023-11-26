import pandas as pd


class DbAdapter:
    def __init__(self, db_url: str):
        # for now, just a placeholder
        print('[INFO] creating db adapter to ' + db_url)
        self.table_stack = {}
        # TODO: all subclasses have mostly similar names, and uses 'table' for 'collection,
        #  this is to streamline method names when plugging into mysql / postgre / influxdb adapters later


class MongoAdapter(DbAdapter):
    import pymongo.database
    import pymongo.collection

    def __init__(self, db_url: str):
        from pymongo import MongoClient
        DbAdapter.__init__(self, db_url)
        self.client = MongoClient(db_url)
        print('[INFO] Mongodb client created')

    def get_db(self, db_name: str) -> pymongo.database.Database:
        # if db_name not in self.table_stack:
        #     self.table_stack[db_name] = {}
        db = self.client[db_name]
        return db

    def get_table(self, db_name: str, table_name: str) -> pymongo.collection.Collection:
        # if table_name not in self.table_stack[db_name]:
        #     self.table_stack[db_name][table_name] = []
        db = self.client[db_name]
        collection = db[table_name]
        return collection

    def insert(self, table: pymongo.collection.Collection, record):
        table.insert_one(record)

    def insert_many(self, table: pymongo.collection.Collection, record_list):
        table.insert_many(record_list)
        print('[INFO] inserted records: ' + str(len(record_list)))

    def get_by_id(self, table: pymongo.collection.Collection, value_for_id: str) -> dict:
        record = table.find_one({'_id': value_for_id})
        return record

    def set_by_id(self, table: pymongo.collection.Collection, value_for_id: str, record: dict):
        edited_dict = dict(record)
        # this replace _id if it exists in the dict
        edited_dict['_id'] = value_for_id
        table.replace_one({'_id': value_for_id}, edited_dict, True)

    def check_table_exists(self, db: pymongo.database.Database, table_name: str) -> bool:
        collection_list = db.list_collection_names()
        collection_exists = False
        if table_name in collection_list:
            collection_exists = True
        return collection_exists

    def sort(self, table: pymongo.collection.Collection, field: str, direction: int, limit: int) -> list:
        if limit == 0:
            sorted_cursor = table.find().sort(key_or_list=field, direction=direction)
        else:
            sorted_cursor = table.find().sort(key_or_list=field, direction=direction).limit(limit)
        # it is risky to return a cursor from a method like this instead of a general purpose list
        sorted_list = []
        for entry in sorted_cursor:
            sorted_list.append(entry)
        return sorted_list

    def create_schedule(self, target_db_name: str, target_tbl_name: str, schedule_type: str, additional_args: dict):
        schedule_tbl = self.get_table('sma', 'advanced_schedule')
        schedule_record = {'db': target_db_name, 'table': target_tbl_name,
                           'type': schedule_type, 'priority': 1, 'status': 'pending'}
        schedule_record = {**schedule_record, **additional_args}
        schedule_tbl.insert_one(schedule_record)


class MongoDS:
    import pymongo.collection
    import pandas.core.frame

    def __init__(self):
        print('MongoDS initialised')

    @classmethod
    def table_to_df(cls, table: pymongo.collection.Collection, field_list: list,  search_dict: dict = None) \
            -> pandas.core.frame.DataFrame:
        if search_dict is None:
            tbl_iter = table.find()
        else:
            tbl_iter = table.find(search_dict)
        # initialise root dict
        root_dict = {}
        for field in field_list:
            root_dict[field] = []
        # iterate over cursor and populate the dict
        for row_dict in tbl_iter:
            for field in field_list:
                root_dict[field].append(row_dict[field])
        # convert to pandas data frame
        df = pd.DataFrame(data=root_dict)
        return df

    @classmethod
    def agg_field_as_dict(cls, table: pymongo.collection.Collection, field: str, search_dict: dict = None) -> dict:
        if search_dict is None:
            tbl_iter = table.find()
        else:
            tbl_iter = table.find(search_dict)
        total_dict = {}
        # iterate the documents provided
        for row_dict in tbl_iter:
            # the dict of key value pairs we need is under 'field', this is what we need.
            info_dict = row_dict[field]
            # iterate through each key and populate count on total dict
            for key in info_dict:
                if key in total_dict:
                    total_dict[key] = total_dict[key] + 1
                else:
                    total_dict[key] = 1
        # return total dict once done
        return total_dict





