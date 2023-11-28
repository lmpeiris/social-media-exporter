from DSJobExecutor import DSJobExecutor
import os
import time
import datetime
import sys
sys.path.insert(0, '../common_lib/')
from DbAdapterClass import MongoAdapter
from DbAdapterClass import Schedule


if __name__ == '__main__':
    # read environmental vars
    DB_SERVER_URL = os.environ['DB_SERVER_URL']
    CYCLE_TIME = 60
    # initialise classes
    db = MongoAdapter(DB_SERVER_URL)
    ds_job = DSJobExecutor(db)
    ds_job.load_locations('sl_gn_dict.pkl')
    # although a schedule object, this is used for querying only; and will not be initialised
    query_scheduler = Schedule(db)
    # ------- main loop ---------
    while True:
        print('[INFO] scheduler cycle started: ' + time.asctime())
        pending_schedules = query_scheduler.list_pending(
            ['keyword_extraction', 'sentiment_analysis', 'location_extraction'])
        for schedule in pending_schedules:
            # create new objects for each schedule and load
            scheduler = Schedule(db)
            scheduler.load_schedule(schedule['_id'])
            scheduler.execute_schedule(ds_job, 'text_extraction')
        print('[INFO] scheduler cycle ended ' + time.asctime())
        time.sleep(CYCLE_TIME)
