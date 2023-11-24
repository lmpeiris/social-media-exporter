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
    reg_tbl = db.get_table('sma', 'advanced_schedule')
    # ------- main loop ---------
    while True:
        print('[INFO] scheduler cycle started: ' + time.asctime())
        # read configuration from mongodb
        for schedule in reg_tbl.find():
            schedule_type = schedule['type']
            # TODO: implement priority based job pick-up
            schedule_priority = schedule['priority']
            if schedule_type in ['keyword_extraction', 'sentiment_analysis', 'location_extraction']:
                db_name = schedule['db']
                tbl_name = schedule['table']
                target_tbl = db.get_table(db_name, tbl_name)
                print('[INFO] executing schedule: ' + schedule_type)
                # getattr allows to call a dynamic method from an object; here object is scheduler.
                report = getattr(scheduler, schedule_type)(target_tbl, schedule['text_field'])
                print('[INFO] ' + schedule_type + ' done on ' + db_name + ',' + tbl_name + '. modified documents: '
                      + str(report['modified_documents']))
        print('[INFO] scheduler cycle ended ' + time.asctime())
        time.sleep(CYCLE_TIME)
