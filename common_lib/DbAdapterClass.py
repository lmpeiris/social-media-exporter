

class DbAdapter:
    def __init__(self, db_url: str):
        # for now, just a placeholder
        print('[INFO] creating db adapter to ' + db_url)
        self.table_stack = {}
        # TODO: all subclasses have mostly similar names, and uses 'table' for 'collection, this is to streamline method names when plugging into mysql / postgre / influxdb adapters later

class MongoAdapter(DbAdapter):
    import pymongo.database
    import pymongo.collection
    def __init__(self, db_url: str):
        from pymongo import MongoClient
        DbAdapter.__init__(self, db_url)
        self.client = MongoClient(db_url)
        print('[INFO] Mongodb client created')

    def get_db(self, db_name: str):
        # if db_name not in self.table_stack:
        #     self.table_stack[db_name] = {}
        db = self.client[db_name]
        return db

    def get_table(self, db_name: str, table_name: str):
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

    def get_by_id(self, table: pymongo.collection.Collection, id: str):
        record = table.find_one({ '_id': id })
        return record

    def set_by_id(self, table: pymongo.collection.Collection, id: str, record: dict):
        edited_dict = dict(record)
        edited_dict['_id'] = id
        table.replace_one({ '_id': id }, edited_dict, True)

    def check_table_exists(self, db: pymongo.database.Database, table_name: str):
        collection_list = db.list_collection_names()
        collection_exists = False
        if table_name in collection_list:
            collection_exists = True
        return collection_exists

    def sort(self, table: pymongo.collection.Collection, field: str, direction: int, limit: int):
        if limit == 0:
            sorted_list = table.find().sort(key_or_list=field, direction=direction)
        else:
            sorted_list = table.find().sort(key_or_list=field, direction=direction).limit(limit)
        return sorted_list