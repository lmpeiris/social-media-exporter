from Scheduler import Scheduler
import os
import time
import datetime
import sys
sys.path.insert(0, '../common_lib/')
from DbAdapterClass import MongoAdapter


if __name__ == '__main__':
    # read environmental vars
    DB_SERVER_URL = os.environ['DB_SERVER_URL']
    CYCLE_TIME = 60
    # initialise classes
    db = MongoAdapter(DB_SERVER_URL)
    scheduler = Scheduler()
    scheduler.load_locations('test.pkl')
    reg_tbl = db.get_table('sma', 'advanced_schedule')
    # ------- main loop ---------
    while True:
        print('[INFO] scheduler cycle started: ' + time.asctime())
        # read configuration from mongodb, add to a list of dicts
        # this decoupling is safe since mongo uses a cursor to iterate, and updates can create issues
        pending_schedules = []
        for schedule in reg_tbl.find({'status': 'pending'}):
            pending_schedules.append(schedule)
        for schedule in pending_schedules:
            print('[DEBUG] running schedule: ' + str(schedule))
            # below object is in bson, no need to decode
            schedule_id = schedule['_id']
            # update schedule as running
            cur_time = datetime.datetime.now().isoformat()
            # TODO: implement priority / created time / status based job pick-up
            schedule_type = schedule['type']
            schedule_priority = schedule['priority']
            db_name = schedule['db']
            tbl_name = schedule['table']
            text_fields = schedule['text_fields']
            target_tbl = db.get_table(db_name, tbl_name)
            task_running = False
            if schedule_type in ['keyword_extraction', 'sentiment_analysis', 'location_extraction']:
                reg_tbl.update_one({'_id': schedule_id}, {'$set': {"status": "running", "modified_time": cur_time}})
                task_running = True
                print('[INFO] executing schedule: ' + schedule_type + ' on')
                # getattr allows to call a dynamic method from an object; here object is scheduler.
                # report = getattr(scheduler, schedule_type)(target_tbl, schedule['text_field'])
                report = scheduler.text_extraction(target_tbl, text_fields, schedule_type)
                print('[INFO] ' + schedule_type + ' done on ' + db_name + ',' + tbl_name + '. modified documents: '
                      + str(report['modified_documents']))
            # marking schedule as processed only if it was processed
            if task_running:
                cur_time = datetime.datetime.now().isoformat()
                reg_tbl.update_one({'_id': schedule_id}, {'$set': {"status": "completed", "modified_time": cur_time}})
        print('[INFO] scheduler cycle ended ' + time.asctime())
        time.sleep(CYCLE_TIME)
