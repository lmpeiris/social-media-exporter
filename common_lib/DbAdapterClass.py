from bson import ObjectId
import pandas as pd
import datetime
import traceback


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

    def insert(self, table: pymongo.collection.Collection, record) -> str:
        insert = table.insert_one(record)
        return insert.inserted_id

    def insert_many(self, table: pymongo.collection.Collection, record_list) -> list:
        insert = table.insert_many(record_list)
        print('[INFO] inserted records: ' + str(len(record_list)))
        return insert.inserted_ids

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

    def sort(self, table: pymongo.collection.Collection, field: str, direction: int, limit: int) -> list[dict]:
        if limit == 0:
            sorted_cursor = table.find().sort(key_or_list=field, direction=direction)
        else:
            sorted_cursor = table.find().sort(key_or_list=field, direction=direction).limit(limit)
        # it is risky to return a cursor from a method like this instead of a general purpose list
        sorted_list = []
        for entry in sorted_cursor:
            sorted_list.append(entry)
        return sorted_list


class Schedule:
    def __init__(self, mongo_adapter: MongoAdapter):
        self.schedule_tbl = mongo_adapter.get_table('sma', 'advanced_schedule')
        self.mongo_adapter = mongo_adapter
        self.schedule_record = {}
        # initial values, must be replaced by running create or load schedule
        self.id_str = '000000000000000000000000'
        self.id = ObjectId('000000000000000000000000')

    def create_schedule(self, schedule_type: str, additional_args: dict,
                        target_db_name: str = None, target_tbl_name: str = None, parent_id: ObjectId = None):
        """Initialises schedule object by creating new one"""
        schedule_record = {'type': schedule_type, 'priority': 1, 'status': 'pending'}
        if target_db_name is not None:
            schedule_record['db'] = target_db_name
            if target_tbl_name is not None:
                # this is a single table based schedule.
                schedule_record['table'] = target_tbl_name
        if parent_id is not None:
            schedule_record['parent_id'] = parent_id
        self.schedule_record = {**schedule_record, **additional_args}
        inserted_record = self.schedule_tbl.insert_one(self.schedule_record)
        self.id_str = inserted_record.inserted_id
        self.id = ObjectId(self.id_str)
        self.schedule_record['_id'] = self.id
        print('[INFO] schedule created: ' + str(inserted_record))

    def load_schedule(self, object_id: ObjectId) -> bool:
        """Initialises schedule object by loading existing one"""
        schedule_record = self.schedule_tbl.find_one({'_id': object_id, 'status': 'pending'})
        if schedule_record is None:
            return False
        else:
            self.id = object_id
            self.id_str = str(object_id)
            self.schedule_record = schedule_record
            return True

    def list_pending(self, schedule_types: list) -> list[dict]:
        """List pending schedules. Use this to find an input for load_schedule"""
        schedule_list = []
        for st in schedule_types:
            for schedule in self.schedule_tbl.find({'status': 'pending', 'type': st}):
                schedule_list.append(schedule)
        print('[INFO] found ' + str(len(schedule_list)) + ' scheduled jobs matching: ' + str(schedule_types))
        return schedule_list

    def execute_schedule(self, object_of_method: object, method_name: str) -> bool:
        """Executes schedule by using the object and the method provided.
        target method should only take schedule dict as arguement"""
        try:
            #TODO: execute load schedule again, to ensure?
            schedule = self.schedule_record
            print('[DEBUG][' + self.id_str + '] picked up schedule: ' + str(schedule))
            # update schedule as running
            cur_time = datetime.datetime.now().isoformat()
            # TODO: implement priority / created time / status based job pick-up
            schedule_type = schedule['type']
            schedule_priority = schedule['priority']

            self.schedule_tbl.update_one({'_id': self.id},
                                         {'$set': {"status": "running", "modified_time": cur_time}})
            print('[INFO][' + self.id_str + '] running ' + method_name + ' on: ' + cur_time)
            # we are passing the entire schedule record to the method
            # method should implement additional db connections etc..
            report = getattr(object_of_method, method_name)(schedule)
            print('[INFO][' + self.id_str + '] ' + schedule_type + ' done on: ' + cur_time + '. Report: ' + str(report))
            cur_time = datetime.datetime.now().isoformat()
            self.schedule_tbl.update_one({'_id': self.id},
                                         {'$set': {"status": "completed", "modified_time": cur_time, "report": report}})
            return True
        except:
            cur_time = datetime.datetime.now().isoformat()
            print('[ERROR][' + self.id_str + '] EXECUTION FAILED!!!!')
            traceback.print_exc()
            self.schedule_tbl.update_one({'_id': self.id},
                                         {'$set': {"status": "failed", "modified_time": cur_time}})
            return False


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





