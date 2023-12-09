from YtHelper import YtHelperMongo
from prometheus_client import start_http_server, Gauge, CollectorRegistry
import time
import os
import pymongo.collection
import sys
sys.path.insert(0, '../common_lib/')
from DbAdapterClass import MongoAdapter
from DbAdapterClass import Schedule
from SMDUtils import SMDUtils


def set_channel_metrics(channel_ids: str):
    # this will pull channel data and update channel stats
    print("[INFO] Getting metrics using get_channel_info")
    channels = yt_api.get_channel_info(channel_id=channel_ids)
    # this will hold channel stats document
    ch_stats = {}
    for channel in channels.items:
        ch_dict = channel.to_dict()
        yt_stats = ch_dict['statistics']
        ch_id = ch_dict['id']
        ch_stats[ch_id] = yt_stats
        ch_title = ch_dict['snippet']['title']
        print("[INFO] Setting metrics for :" + ch_title)
        ch_view_count.labels(channel=ch_title).set(yt_stats['viewCount'])
        ch_subs_count.labels(channel=ch_title).set(yt_stats['subscriberCount'])
        ch_video_count.labels(channel=ch_title).set(yt_stats['videoCount'])
        # print('upload video playlist id: ' + ch_dict['contentDetails']['relatedPlaylists']['uploads'] )
        print("[DEBUG] metrics: " + ch_id + '|' + str(yt_stats))
    if ENABLE_DB_BACKEND:
        # WARN: don't just use 'stats' it's a reserved word in mongodb!!
        stats_tbl = db.get_table('sma_yt', 'yt_stats')
        db.set_by_id(stats_tbl, 'ch_stats', ch_stats)


def set_video_metrics(playlist_ids: list):
    # this function will pull video data from given playlist ids
    # check whether we have run this already, by checking for table existence
    table_suffix = SMDUtils.get_daily_prefix()
    stats_tbl_name = 'vid_stat_' + table_suffix
    sma_yt_db = db.get_db('sma_yt')
    table_exists = db.check_table_exists(sma_yt_db, stats_tbl_name)
    if table_exists:
        print('[INFO] video data is already loaded for the day. skipping')
    else:
        # Get video ids from playlist ids. please refer YtHelper.py for this method
        video_id_list = yt_api.sma_get_vid_ids_from_pl(playlist_ids)
        print('[INFO] getting details of videos')
        vid_tbl = db.get_table('sma_yt', 'videos')
        vid_stat_tbl = db.get_table('sma_yt', stats_tbl_name)
        # get video data per video. please refer YtHelper.py for this method
        vid_doc_list, vid_stat_list = yt_api.sma_get_video_data(video_id_list)
        # update database records
        # TODO: no need to over-write video data once retrived.
        for i in vid_doc_list:
            db.set_by_id(vid_tbl, i['_id'], i)
        # stats are freshly added per-collection
        vid_stat_tbl.insert_many(vid_stat_list)
        # picking sorted videos
        sort_videos(vid_tbl, vid_stat_tbl)


def sort_videos(vid_tbl: pymongo.collection.Collection, vid_stat_tbl: pymongo.collection.Collection):
    vid_stats = {}
    most_popular = []
    print('[INFO] getting top 5 viewed videos from database')
    most_viewed_cur = db.sort(vid_stat_tbl, 'viewCount', -1, 5)
    print('[INFO] setting metrics')
    for record in most_viewed_cur:
        video_id = record['_id']
        # setting metrics
        pop_vid_views.labels(video_id=video_id).set(record['viewCount'])
        pop_vid_likes.labels(video_id=video_id).set(record['likeCount'])
        pop_vid_comments.labels(video_id=video_id).set(record['commentCount'])
        most_popular.append(video_id)
    print('[INFO] most popular videos are: ' + str(most_popular))
    print('[INFO] getting latest 5 videos from database')
    latest_videos = []
    newest_videos_cur = db.sort(vid_tbl, 'publishedAt', -1, 5)
    for record in newest_videos_cur:
        video_id = record['_id']
        latest_videos.append(video_id)
    # TODO: implement ISO 8601 durations decode logic as utils method, for analysis on duration
    print('[INFO] latest videos are: ' + str(latest_videos))
    # adding to vid stats
    vid_stats['most_popular'] = most_popular
    vid_stats['latest_videos'] = latest_videos
    stats_tbl = db.get_table('sma_yt', 'yt_stats')
    db.set_by_id(stats_tbl, 'most_viewed_vids', vid_stats)
    # populate comment threads (ct)/ comments, only for the most popular.
    ct_tbl = db.get_table('sma_yt', 'ct')
    comments_tbl = db.get_table('sma_yt', 'comments')
    # TODO: create a logic to avoid reloading by checking video id on ct table
    for i in most_popular:
        ct_list, comment_list = yt_api.sma_get_comments_for_vid(i)
        # replace records if exists
        for x in ct_list:
            ct_tbl.replace_one({'_id': x['_id']}, x, True)
        for y in comment_list:
            comments_tbl.replace_one({'_id': y['_id']}, y, True)


if __name__ == '__main__':
    # read environmental vars
    YT_API_KEY = os.environ['YT_API_KEY']
    CHANNEL_IDS = os.environ['CHANNEL_IDS']
    DB_SERVER_URL = os.environ['DB_SERVER_URL']
    # enabling this will pull video data, if not already present.
    ENABLE_VID_STATS = SMDUtils.env_bool('ENABLE_VID_STATS')
    PLAYLIST_IDS = os.environ['PLAYLIST_IDS'].split(',')
    # enable maintaining stats in mongodb. Required for advanced ML pods to work
    ENABLE_DB_BACKEND = SMDUtils.env_bool('ENABLE_DB_BACKEND')
    # how fast the program pulls data
    CYCLE_TIME = 600
    ############################################
    # creating python-youtube client based custom class - object
    # TODO: detect error when session times out and handle reconnect
    yt_api = YtHelperMongo(api_key=YT_API_KEY)
    # creating mongodb connection
    db = MongoAdapter(DB_SERVER_URL)
    yt_api.set_mongo(db)
    # create new registry for our metrics.
    # This also allows to suppress exposing default python process metrics via default registry 'REGISTRY'
    yt_reg = CollectorRegistry()
    # initialise metrics
    # syntax: Gauge(var_name, var_help, list of labels we can choose when setting var, registry)
    # channel, uses channel title as label
    ch_view_count = Gauge('ch_view_count', 'Total view count of the channel', ["channel"], registry=yt_reg)
    ch_subs_count = Gauge('ch_subs_count', 'Total subscribers of the channel', ["channel"], registry=yt_reg)
    ch_video_count = Gauge('ch_video_count', 'Total videos of the channel', ["channel"], registry=yt_reg)
    # videos, uses video id as label
    # for top 5 videos, by views
    pop_vid_views = Gauge('pop_vid_views', 'video views of top 5 videos', ["video_id"], registry=yt_reg)
    pop_vid_comments = Gauge('pop_vid_comments', 'video comments of top 5 videos', ["video_id"], registry=yt_reg)
    pop_vid_likes = Gauge('pop_vid_likes', 'video likes of top 5 videos', ["video_id"], registry=yt_reg)

    # starting prom exporter server, it will automatically serve all defined metrics
    start_http_server(9130, registry=yt_reg)
    while True:
        set_channel_metrics(CHANNEL_IDS)
        set_video_metrics(PLAYLIST_IDS)
        # check for jobs to pick up from the scheduler
        query_scheduler = Schedule(db)
        pending_schedules = query_scheduler.list_pending(['search_download'])
        for schedule in pending_schedules:
            scheduler = Schedule(db)
            scheduler.load_schedule(schedule['_id'])
            scheduler.execute_schedule(yt_api, 'sma_search_download')
        print('[INFO] current cycle ended, sleeping processing thread for seconds: ' + str(CYCLE_TIME))
        time.sleep(CYCLE_TIME)

