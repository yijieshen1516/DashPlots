import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
from azure.storage.blob import ContainerClient
from io import BytesIO
import datetime

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options

pasthours_list = [2, 3, 4, 5, 6, 7, 8, 9, 10]
param_list = ['pressure', 'rate', 'engineRPM', 'gear']

app.layout = html.Div([
        html.Div([
            dcc.Dropdown(
                id='hour_list',
                options=[{'label': i, 'value': i} for i in pasthours_list],
                value=2
            ),
        ],style={'width': '40%', 'display': 'inline-block'}),

        html.Div([
            dcc.Dropdown(
                id='acp_list'
            ),
        ],
        style={'width': '30%', 'display': 'inline-block'}),

        html.Div([
            dcc.Dropdown(
                id='param_list',
                options=[{'label': i, 'value': i} for i in param_list],
                value='pressure'
            ),
        ],
        style={'width': '30%', 'float': 'right', 'display': 'inline-block'}),

        dcc.Graph(id='spreadstate_plot'),
        dcc.Graph(id='pump_plot'),
    ])


@app.callback(
    [Output('acp_list', 'options'),
     Output('acp_list', 'value')],
    Input('hour_list', 'value'))
def set_acps_options(selected_hour):

    timestamp = datetime.datetime.utcnow() - datetime.timedelta(hours=selected_hour)
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day
    hour = timestamp.hour

    prefix = f'sandy/pump/year={year}/month={month}/day={day}/hour={hour}/'

    container_name = 'kelvin-processed'
    conn_string = 'DefaultEndpointsProtocol=https;AccountName=equipmentdata;AccountKey=+PAI2pc7BwkpdHy70c0lDm7Fml7nk9+W9qTVgwDJZJRago5/zWuckvTXN6Q2KA9YxarwnvcVarKkHPXaycPgng==;EndpointSuffix=core.windows.net'

    # old way to read blobs and doesn't work well with parquet file
    # account_name = 'equipmentdata'
    # account_key = '+PAI2pc7BwkpdHy70c0lDm7Fml7nk9+W9qTVgwDJZJRago5/zWuckvTXN6Q2KA9YxarwnvcVarKkHPXaycPgng=='
    # block_blob_service = BlockBlobService(account_name= account_name, account_key= account_key)
    # blobs = block_blob_service.list_blobs(container_name, prefix="sandy/pump/year=2021/month=3/day=16/hour=13/")
    # file_names = [blob for blob in blobs if blob.name.endswith('.parquet')]
    # blob_item = block_blob_service.get_blob_to_bytes(container_name, file_names[0].name)
    # df = pd.read_parquet(StringIO(blob_item.content), engine='pyarrow')

    container = ContainerClient.from_connection_string(conn_str=conn_string, container_name=container_name)
    blobs = container.list_blobs(name_starts_with=prefix)
    blob_names = [blob for blob in blobs if blob.name.endswith('.parquet')]

    blob_client = container.get_blob_client(blob=blob_names[0].name)
    stream_downloader = blob_client.download_blob()
    stream = BytesIO()
    stream_downloader.readinto(stream)
    processed_df = pd.read_parquet(stream, engine='pyarrow')

    acp_list = processed_df['acp'].unique().tolist()

    return [{'label': i, 'value': i} for i in acp_list], acp_list[0]


@app.callback(
    Output('pump_plot', 'figure'),
    Input('hour_list', 'value'),
    Input('acp_list', 'value'),
    Input('param_list', 'value'))
def update_graph(selected_hour, selected_acp, selected_param):

    timestamp = datetime.datetime.utcnow() - datetime.timedelta(hours=selected_hour)
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day
    hour = timestamp.hour

    prefix = f'sandy/pump/year={year}/month={month}/day={day}/hour={hour}/'

    container_name = 'kelvin-processed'
    conn_string = 'DefaultEndpointsProtocol=https;AccountName=equipmentdata;AccountKey=+PAI2pc7BwkpdHy70c0lDm7Fml7nk9+W9qTVgwDJZJRago5/zWuckvTXN6Q2KA9YxarwnvcVarKkHPXaycPgng==;EndpointSuffix=core.windows.net'

    container = ContainerClient.from_connection_string(conn_str=conn_string, container_name=container_name)
    blobs = container.list_blobs(name_starts_with=prefix)
    blob_names = [blob for blob in blobs if blob.name.endswith('.parquet')]

    blob_client = container.get_blob_client(blob=blob_names[0].name)
    stream_downloader = blob_client.download_blob()
    stream = BytesIO()
    stream_downloader.readinto(stream)
    processed_df = pd.read_parquet(stream, engine='pyarrow')

    df = processed_df[processed_df['acp'] == selected_acp]

    df['pumptime'] = pd.to_datetime(df['timestamp'], unit='s', origin='unix')
    fig = px.scatter(df, x='pumptime', y=selected_param,
                     facet_col='sapId', facet_col_wrap=4)

    #df = px.data.gapminder()
    #fig = px.scatter(df, x='gdpPercap', y='lifeExp', color='continent', size='pop',
    #                 facet_col='year', facet_col_wrap=4)

    return fig


@app.callback(
    Output('spreadstate_plot', 'figure'),
    Input('hour_list', 'value'),
    Input('acp_list', 'value'),
    Input('param_list', 'value'))
def update_graph(selected_hour, selected_acp, selected_param):

    timestamp = datetime.datetime.utcnow() - datetime.timedelta(hours=selected_hour)
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day
    hour = timestamp.hour

    prefix = f'sandy/pump/year={year}/month={month}/day={day}/hour={hour}/'

    container_name = 'kelvin-processed'
    conn_string = 'DefaultEndpointsProtocol=https;AccountName=equipmentdata;AccountKey=+PAI2pc7BwkpdHy70c0lDm7Fml7nk9+W9qTVgwDJZJRago5/zWuckvTXN6Q2KA9YxarwnvcVarKkHPXaycPgng==;EndpointSuffix=core.windows.net'

    container = ContainerClient.from_connection_string(conn_str=conn_string, container_name=container_name)
    blobs = container.list_blobs(name_starts_with=prefix)
    blob_names = [blob for blob in blobs if blob.name.endswith('.parquet')]

    blob_client = container.get_blob_client(blob=blob_names[0].name)
    stream_downloader = blob_client.download_blob()
    stream = BytesIO()
    stream_downloader.readinto(stream)
    processed_df = pd.read_parquet(stream, engine='pyarrow')

    df = processed_df[processed_df['acp'] == selected_acp]

    df['pumptime'] = pd.to_datetime(df['timestamp'], unit='s', origin='unix')

    param = 'spreadPressure'

    if selected_param == 'pressure':
        param = 'spreadPressure'
    elif selected_param == 'rate':
        param = 'spreadRate'
    elif selected_param == 'engineRPM':
        param = 'spreadPressure'
    elif selected_param == 'gear':
        param = 'spreadPressure'

    df = df[['pumptime', param]].drop_duplicates()

    fig = px.scatter(df, x='pumptime', y=param)

    return fig


if __name__ == '__main__':
    app.run_server(debug=True, port=8000)

