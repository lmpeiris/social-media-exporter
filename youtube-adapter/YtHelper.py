from pyyoutube import Api
import sys
import datetime
sys.path.insert(0, '../common_lib/')
from DbAdapterClass import MongoAdapter

class YtHelper(Api):
    # standard super-class init
    def __init__(self, *args, **kwargs):
        super(YtHelper, self).__init__(*args, **kwargs)

    def sma_get_vid_ids_from_pl(self, playlist_ids: list) -> list:
        # first, you need to get 'upload' playlist's items, retrieve the video id from there, and then get the status
        video_id_list = []
        for playlist_id in playlist_ids:
            print('[INFO] Getting playlist items: ' + playlist_id)
            playlist_items = self.get_playlist_items(playlist_id=playlist_id, count=None)
            print('[INFO] Got playlist items: ' + str(len(playlist_items.items)) + ' proceeding to get video data')
            for playlist_item in playlist_items.items:
                playlist_item_dict = playlist_item.to_dict()
                video_id = playlist_item_dict['contentDetails']['videoId']
                video_id_list.append(video_id)
        return video_id_list

    def sma_get_video_data(self, video_id_list: list) -> tuple[list[dict], list[dict]]:
        """returns vid_doc_list, vid_stat_list both list of dicts"""
        # we will create a list, and use multi insert for efficiency
        vid_doc_list = []
        vid_stat_list = []
        insert_on = datetime.datetime.utcnow()
        for video_id in video_id_list:
            print('[DEBUG] getting details for video: ' + video_id)
            video = self.get_video_by_id(video_id=video_id)
            video_dict = video.items[0].to_dict()
            vid_append_dict = {}
            vid_append_dict['_id'] = video_id
            vid_append_dict['insert_on'] = insert_on
            vid_append_dict['duration'] = video_dict['contentDetails']['duration']
            for i in ['title', 'channelId', 'publishedAt', 'description']:
                vid_append_dict[i] = video_dict['snippet'][i]
            # updating data in video collection
            vid_doc_list.append(vid_append_dict)
            # writing data to new video stats table
            yt_video_stats = dict(video_dict['statistics'])
            yt_video_stats['_id'] = video_id
            vid_stat_list.append(yt_video_stats)
        return vid_doc_list, vid_stat_list

    def sma_get_comments_for_vid(self, video_id: str) -> tuple[list, list]:
        """returns ct_list, comment_list both list of dicts"""
        # get comment threads per video
        # comments threads are the top-level comments for a video.
        print('[INFO] Getting comment data for video id: ' + video_id)
        comment_threads = self.get_comment_threads(video_id=video_id, count=None)
        ct_list = []
        comment_list = []
        insert_on = datetime.datetime.utcnow()
        print('[INFO] Received comment threads: ' + str(len(comment_threads.items)) + ' for video: ' + video_id)
        for ct in comment_threads.items:
            ct_dict = ct.to_dict()
            ct_id = ct_dict['id']
            # this is the dict saved to the database
            ct_saved_dict = {}
            ct_saved_dict['_id'] = ct_id
            ct_saved_dict['insert_on'] = insert_on
            snippet = ct_dict['snippet']['topLevelComment']['snippet']
            ct_saved_dict['authorChannelId'] = snippet['authorChannelId']['value']
            ct_saved_dict['video_id'] = video_id
            # initialise
            ct_saved_dict['totalReplyCount'] = 0
            for i in ['textOriginal', 'likeCount', 'updatedAt']:
                ct_saved_dict[i] = snippet[i]
            # get comments per comment thread, if available
            if ct_dict['replies'] is not None:
                ct_saved_dict['totalReplyCount'] = ct_dict['snippet']['totalReplyCount']
                comments = ct_dict['replies']['comments']
                for comment in comments:
                    comment_id = comment['id']
                    comment_saved_dict = {}
                    comment_saved_dict['_id'] = comment_id
                    comment_saved_dict['insert_on'] = insert_on
                    comment_saved_dict['video_id'] = video_id
                    comment_saved_dict['ct_id'] = ct_id
                    com_snip = comment['snippet']
                    comment_saved_dict['authorChannelId'] = com_snip['authorChannelId']['value']
                    for i in ['textOriginal', 'likeCount', 'updatedAt']:
                        comment_saved_dict[i] = com_snip[i]
                    # append comment
                    comment_list.append(comment_saved_dict)
            # append comment thread
            ct_list.append(ct_saved_dict)
        print('[INFO] found replies: ' + str(len(comment_list)) + ' for video id: ' + video_id)
        # return as tuple
        return ct_list, comment_list

    def sma_search_videos(self, search_term: str, limit: int, search_criteria: dict = None) -> tuple[list, list[dict]]:
        """ returns return_id_list, return_dict_list
        sample criteria = 'location':'6.9271, 79.8612','location_radius':'10mi',
        'published_after':'2022-07-08T00:00:00Z', 'published_before':'2022-07-10T00:00:00Z'"""
        print('[INFO] searching for ' + str(limit) + ' videos on: ' + search_term)
        if search_criteria is None:
            sr_list = self.search(q=search_term, count=limit, search_type="video")
        else:
            print('[DEBUG] search parameters are: ' + str(search_criteria))
            sr_list = self.search(location=search_criteria['location'],
                                  location_radius=search_criteria['location_radius'],
                                  q=search_term, parts=["snippet"], count=limit, search_type="video",
                                  published_after=search_criteria['published_after'],
                                  published_before=search_criteria['published_before'])
        return_dict_list = []
        return_id_list = []
        for sr in sr_list.items:
            vid_dict = sr.to_dict()
            vid_append = {}
            vid_id = vid_dict['id']['videoId']
            vid_append['_id'] = vid_id
            return_id_list.append(vid_id)
            snippet = vid_dict['snippet']
            for i in ['publishedAt', 'channelId', 'title', 'description', 'channelTitle']:
                vid_append[i] = snippet[i]
            return_dict_list.append(vid_append)
        return return_id_list, return_dict_list


class YtHelperMongo(YtHelper):
    """Use set_mongo to initialise properly"""
    # standard super-class init

    def __init__(self, *args, **kwargs):
        super(YtHelperMongo, self).__init__(*args, **kwargs)
        # TODO: this is a hack, and a bad practice
        self.mongo_adapter = MongoAdapter('mongodb://127.0.0.1')

    def set_mongo(self, db: MongoAdapter):
        self.mongo_adapter = db

    def sma_download_vid_data(self, video_id_list: list, db_name: str, vid_tbl_name: str,
                              ct_tbl_name: str, comments_tbl_name: str) -> dict:
        """Download video data and write to db.
        video id list is a list of strings
        returns download stats"""
        vid_table = self.mongo_adapter.get_table(db_name, vid_tbl_name)
        ct_table = self.mongo_adapter.get_table(db_name, ct_tbl_name)
        comments_table = self.mongo_adapter.get_table(db_name, comments_tbl_name)
        vid_id_exist = []
        vid_id_not_exist = []
        # remember that video ids are not object ids when storing in mongodb
        for video in vid_table.find({"_id": {"$in": video_id_list}}, {"_id": 1}):
            # although we re only taking _id, video is still a dict
            vid_id_exist.append(video['_id'])
        for video in video_id_list:
            if video not in vid_id_exist:
                vid_id_not_exist.append(video)
            else:
                print('[DEBUG] skipping since already downloaded: ' + video)
        vid_doc_list, vid_stat_list = self.sma_get_video_data(vid_id_not_exist)
        print('[INFO] inserting to video table')
        vid_table.insert_many(vid_doc_list)
        # assume if video data not there, then comments also not
        ct_final_list = []
        comments_final_list = []
        for video_id in vid_id_not_exist:
            ct_list, comment_list = self.sma_get_comments_for_vid(video_id)
            # lists cannot be appended as whole, need to iterate
            for ct in ct_list:
                ct_final_list.append(ct)
            for comment in comment_list:
                comments_final_list.append(comment)
        print('[INFO] inserting to ct table')
        ct_table.insert_many(ct_final_list)
        print('[INFO] inserting to comments table')
        comments_table.insert_many(comments_final_list)
        report = {'vid_down_data': {'videos': str(len(vid_id_not_exist)), 'cts': str(len(ct_final_list)),
                                    'comments': str(len(comments_final_list))}}
        return report

    def sma_search_download(self, schedule):
        video_id_list, video_dict_list = self.sma_search_videos(schedule['search_term'], schedule['limit'],
                                                                schedule['search_criteria'])
        # download vid data requires a list string of ids
        down_report = self.sma_download_vid_data(video_id_list, schedule['db'], 'videos','ct', 'comments')
        report = {'video_list': video_id_list, **down_report}
        return report

