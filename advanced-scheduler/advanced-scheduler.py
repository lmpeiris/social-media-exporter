from DbAdapterClass import MongoAdapter
from Scheduler import Scheduler
import os
import time


if __name__ == '__main__':
    # read environmental vars
    DB_SERVER_URL = os.environ['DB_SERVER_URL']
    CYCLE_TIME = 60
    # initialise classes
    db = MongoAdapter(DB_SERVER_URL)
    scheduler = Scheduler()
    scheduler.load_locations('sl_locations.pkl')
    reg_tbl = db.get_table('sma', 'advanced_schedule')
    # ------- main loop ---------
    while True:
        print('[INFO] scheduler cycle started: ' + time.asctime())
        # read configuration from mongodb
        for schedule in reg_tbl.find():
            print('[DEBUG] found schedule: ' + str(schedule))
            schedule_type = schedule['type']
            # TODO: implement priority based job pick-up
            schedule_priority = schedule['priority']
            # below value is in bson, but not an issue for is
            schedule_id = schedule['_id']
            if schedule_type in ['keyword_extraction', 'sentiment_analysis', 'location_extraction']:
                db_name = schedule['db']
                tbl_name = schedule['table']
                target_tbl = db.get_table(db_name, tbl_name)
                print('[INFO] executing schedule: ' + schedule_type)
                # getattr allows to call a dynamic method from an object; here object is scheduler.
                # report = getattr(scheduler, schedule_type)(target_tbl, schedule['text_field'])
                report = scheduler.text_extraction(target_tbl, schedule['text_field'], schedule_type)
                print('[INFO] ' + schedule_type + ' done on ' + db_name + ',' + tbl_name + '. modified documents: '
                      + str(report['modified_documents']))
            # removing schedule regardless whether it was processed or not
            reg_tbl.delete_one({'_id': schedule_id})
        print('[INFO] scheduler cycle ended ' + time.asctime())
        time.sleep(CYCLE_TIME)
