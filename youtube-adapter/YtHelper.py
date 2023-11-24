from pyyoutube import Api


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

    def sma_get_video_data(self, video_id_list: list) -> tuple[list, list]:
        # we will create a list, and use multi insert for efficiency
        vid_doc_list = []
        vid_stat_list = []
        for video_id in video_id_list:
            print('[DEBUG] getting details for video: ' + video_id)
            video = self.get_video_by_id(video_id=video_id)
            video_dict = video.items[0].to_dict()
            vid_append_dict = {}
            vid_append_dict['_id'] = video_id
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
        # get comment threads per video
        # comments threads are the top-level comments for a video.
        print('[INFO] Getting comment data for video id: ' + video_id)
        comment_threads = self.get_comment_threads(video_id=video_id, count=None)
        ct_list = []
        comment_list = []
        print('[INFO] Received comment threads: ' + str(len(comment_threads.items)) + ' for video: ' + video_id)
        for ct in comment_threads.items:
            ct_dict = ct.to_dict()
            ct_id = ct_dict['id']
            # this is the dict saved to the database
            ct_saved_dict = {}
            ct_saved_dict['_id'] = ct_id
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

    def sma_search_videos(self, search_term: str, search_criteria: dict, limit: int) -> list:
        # sample criteria
        # 'location':'6.9271, 79.8612','location_radius':'10mi',
        # 'published_after':'2022-07-08T00:00:00Z', 'published_before':'2022-07-10T00:00:00Z'
        sr_list = self.search(location=search_criteria['location'], location_radius=search_criteria['location_radius'],
                                q=search_term, parts=["snippet"], count=limit, search_type="video",
                                published_after=search_criteria['published_after'],
                                published_before=search_criteria['published_before'])
        return_list = []
        for sr in sr_list.items:
            vid_dict = sr.to_dict()
            vid_append = {}
            vid_append['_id'] = vid_dict['id']['videoId']
            snippet = vid_dict['snippet']
            for i in ['publishedAt', 'channelId', 'title', 'description', 'channelTitle']:
                vid_append[i] = snippet[i]
            return_list.append(vid_append)
        return return_list
