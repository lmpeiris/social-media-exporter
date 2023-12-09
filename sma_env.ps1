##### YOUTUBE ##########
# youtube data api v3 key
$env:YT_API_KEY='xxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
# comma separated list of channel ids and upload playlist ids
$env:CHANNEL_IDS='aaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbb,ccccccccccccccccc'
$env:PLAYLIST_IDS='ddddddddddd,eeeeeeeeeeeeee,fffffffffffffff'
# enabling this will also expose metrics for 5 most popular and 5 latest vids
$env:ENABLE_VID_STATS='True'

######### DB ##########
# database backend (except prometheus client which works by default)
$env:ENABLE_DB_BACKEND='True'
$env:DB_SERVER_URL='mongodb://127.0.0.1:27017/'

######### SCHEDULE ############
# how fast the program pulls data
$env:CYCLE_TIME='3600'