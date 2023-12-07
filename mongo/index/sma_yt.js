use sma_yt
db.ct.createIndex( { video_id: -1 } )
db.comments.createIndex( { video_id: -1 } )