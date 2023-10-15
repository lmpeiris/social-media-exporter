from datetime import datetime
import os


class SMDUtils:
    def __init__(self):
        # nothing much to do here still
        print('[INFO] loaded SMDUtils')

    @classmethod
    def get_daily_prefix(cls):
        cur_time = datetime.now()
        suffix = cur_time.strftime("%Y_%m_%d")
        return suffix

    @classmethod
    def get_hourly_prefix(cls):
        cur_time = datetime.now()
        suffix = cur_time.strftime("%Y_%m_%d_%H")
        return suffix

    @classmethod
    def env_bool(cls, env_bool_str: str):
        bool_value = (os.getenv(env_bool_str, 'False').lower() == 'true')
        return bool_value