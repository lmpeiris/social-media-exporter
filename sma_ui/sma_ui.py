# Import packages
from dash import Dash, html, dash_table, dcc, callback, Output, Input
from bson import ObjectId
import plotly.express as px
import folium
import sys
sys.path.insert(0, '../common_lib/')
from DbAdapterClass import MongoAdapter
from DbAdapterClass import MongoDS

# initialise db connection
db = MongoAdapter('mongodb://172.22.233.20:27017')
schedule_tbl = db.get_table('sma', 'advanced_schedule')
# get table data from schedule
schedule_df = MongoDS.table_to_df(schedule_tbl, ['_id', 'type', 'status', 'modified_time'],{}
                                  ,['_id'])
# get only list f schedule ids
schedule_id_list = list(schedule_df['_id'])

# Initialize the app
app = Dash(__name__)

# App layout
app.layout = html.Div([
    html.Div(children='SMA_UI: visualise social media data with geospatial meaning'),
    html.Hr(),
    # dash-table no longer have reliable support for dataframes, see https://github.com/plotly/dash-table/issues/912
    # use dicts directly
    dash_table.DataTable(data=schedule_df.to_dict('records'), page_size=6),
    dcc.Dropdown(schedule_id_list, schedule_id_list[0], id='schedule-dropdown'),
    html.Iframe(id='map', srcDoc=open('folium-map.html', 'r').read(), width='100%', height='600')
])


@callback(
    Output(component_id='map', component_property='height'),
    Input(component_id='schedule-dropdown', component_property='value')
)
def update_folium(schedule_id_str):
    print('[INFO] recreating map for: ' + schedule_id_str)
    #TODO: read report and get the coordinates
    #schedule_record = schedule_tbl.find_one({'_id': ObjectId(schedule_id_str)})
    #TODO: set them on map
    # TODO: get the centeroid of loctions, for now set this
    centeroid = (6.8412, 79.9654)
    fl_map = folium.Map(location=centeroid, zoom_start=18)
    fl_map.save('folium-map.html')
    # no return required, just reloading map from same location enough
    return '600'


# Run the app
if __name__ == '__main__':
    app.run(debug=True)
