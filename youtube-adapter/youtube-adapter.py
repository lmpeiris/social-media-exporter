import time
import os
import sys
from prometheus_client import start_http_server, Gauge, CollectorRegistry
from pyyoutube import Api
# Custom classes from here below
sys.path.insert(0, '../common_lib/')
from DbAdapterClass import MongoAdapter
from SMDUtils import SMDUtils

def set_channel_metrics(channel_ids: str):
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
        # WARN: don't just use 'stats' its a reserved word!!
        stats_tbl = db.get_table('sma_yt', 'yt_stats')
        db.set_by_id(stats_tbl, 'ch_stats', ch_stats)

def set_video_metrics(playlist_ids: list):
    # check whether we have run this already, by checking for table existence
    table_suffix = SMDUtils.get_daily_prefix()
    table_name = 'vid_stat_' + table_suffix
    sma_yt_db = db.get_db('sma_yt')
    table_exists = db.check_table_exists(sma_yt_db,table_name)
    if table_exists:
        print('[INFO] video data is already loaded. skipping')
    else:
        # first, you need to get 'upload' playlist's items, retrieve the video id from there, and then get the status
        video_id_list = []
        for playlist_id in playlist_ids:
            print('[INFO] Getting playlist items: ' + playlist_id)
            playlist_items = yt_api.get_playlist_items(playlist_id=playlist_id, count=None)
            print('[INFO] Got playlist items: ' + str(len(playlist_items.items)) + ' proceeding to get video data')
            for playlist_item in playlist_items.items:
                playlist_item_dict = playlist_item.to_dict()
                video_id = playlist_item_dict['contentDetails']['videoId']
                video_id_list.append(video_id)

        print('[INFO] getting details of videos')
        vid_tbl = db.get_table('sma_yt','videos')
        # we will create a list, and use multi insert for efficiency
        vid_stat_list = []
        for video_id in video_id_list:
            print('[DEBUG] getting details for video: ' + video_id)
            video = yt_api.get_video_by_id(video_id=video_id)
            video_dict = video.items[0].to_dict()
            db_vid_dict = {}
            db_vid_dict['duration'] = video_dict['contentDetails']['duration']
            for i in [ 'title', 'channelId', 'publishedAt']:
                db_vid_dict[i] = video_dict['snippet'][i]
            # updating data in video collection
            db.set_by_id(vid_tbl,video_id, db_vid_dict)
            # writing data to new video stats table
            yt_video_stats = dict(video_dict['statistics'])
            yt_video_stats['_id'] = video_id
            vid_stat_list.append(yt_video_stats)
        vid_stat_tbl = db.get_table('sma_yt', table_name)
        db.insert_many(vid_stat_tbl, vid_stat_list)
        # picking sorted videos
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
    CYCLE_TIME = 86400
    ############################################

    # creating mongodb connection
    if ENABLE_DB_BACKEND:
        db = MongoAdapter(DB_SERVER_URL)
    # creating python-youtube client
    # TODO: detect error when session times out and handle reconnect
    yt_api = Api(api_key=YT_API_KEY)
    # create new registry for our metrics.
    # This also allows to supress exposing default python process metrics via default registry 'REGISTRY'
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
        # video metrics not supported without mongodb backend
        if ENABLE_DB_BACKEND:
            set_video_metrics(PLAYLIST_IDS)
        print('[INFO] current cycle ended, sleeping processing thread for seconds: ' + str(CYCLE_TIME))
        time.sleep(CYCLE_TIME)

