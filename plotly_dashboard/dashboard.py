import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import dash_table
import boto3
import plotly.graph_objs as go
from io import StringIO

s3 = boto3.client('s3')


def read_csv_from_s3(s3_path):
    bucket_name, file_key = s3_path.replace('s3://', '').split('/', 1)
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = obj['Body']
    csv_string = body.read().decode('utf-8')
    data_df = pd.read_csv(StringIO(csv_string))
    return data_df


S3_PATH = 's3://spotifyoutputdataset/output/tracks.csv'

data_df = read_csv_from_s3(S3_PATH)

app = dash.Dash(__name__)

artist_options = [{'label': artist, 'value': artist}
                  for artist in data_df['artists'].unique()]

styles = {
    'container': {
        'width': '100%',
        'margin': '0 auto'
    },
    'title': {
        'textAlign': 'center',
        'fontSize': '36px',
        'marginBottom': '20px'
    },
    'dropdown': {
        'width': '50%',
        'margin': '0 auto',
        'textAlign': 'center'
    },
    'content': {
        "width": "100%",
        'display': 'flex',
        'justifyContent': 'space-between',
    },
    'table': {
        'width': '45%',
        'margin': '20px auto'
    },
    'chart': {
        'width': '50%',
        'margin': '20px auto'
    }
}

app.layout = html.Div(
    className="container",
    children=[
        html.Div(id='page-content'),
        html.H1("Spotify Data Analysis", className="title"),
        html.Div(
            className="graphs-container",
            children=[
                html.Div(
                    className='row',
                    children=[
                        html.Div(dcc.Graph(id='artists_graph'),
                                 className='col visualization'),
                        html.Div(dcc.Graph(id='artists_tracks_graph'),
                                 className='col visualization'),
                    ]
                ),
                html.Div(
                    className='row',
                    children=[
                        html.Div(dcc.Graph(id='songs_graph'),
                                 className='col visualization'),
                        html.Div(dcc.Graph(id='album_graph'),
                                 className='col visualization'),
                    ]
                ),
                html.Div(
                    className='row',
                    children=[
                        html.Div(dcc.Graph(id='top_albums_graph'),
                                 className='col visualization'),
                        html.Div(
                            dcc.Graph(id='popularity_over_time_graph'), className='col visualization'),
                    ]
                ),
                html.Div(
                    className='row',
                    children=[
                        html.Div(dcc.Graph(id='top_tracks_graph'),
                                 className='col visualization'),
                        html.Div(dcc.Graph(id='top_artists_graph'),
                                 className='col visualization'),
                    ]
                ),

                # artist selection and song display
                html.Div(
                    style=styles['container'],
                    children=[
                        html.H1("Artist Dashboard", style=styles['title']),
                        html.Div(
                            className="graphs-container",
                            children=[
                                html.Div(
                                    className='row',
                                    children=[
                                        html.Div(
                                            style=styles['dropdown'],
                                            children=[
                                                html.Label("Select Artist:", style={
                                                    'fontSize': '24px'}),
                                                dcc.Dropdown(
                                                    id='artist-dropdown',
                                                    options=artist_options,
                                                    value=artist_options[0]['value']
                                                )
                                            ]
                                        )
                                    ]
                                ),
                                html.Div(
                                    style=styles['content'],
                                    children=[
                                        html.Div(
                                            style=styles['table'],
                                            id='song-table'
                                        ),
                                        html.Div(
                                            style=styles['chart'],
                                            id='song-chart'
                                        )
                                    ]
                                )
                            ]
                        )
                    ]
                )
            ]
        )
    ]
)


@app.callback(
    Output('song-table', 'children'),
    [Input('artist-dropdown', 'value')]
)
def update_song_table(selected_artist):
    artist_data = data_df[data_df['artists'] == selected_artist]

    song_table = dash_table.DataTable(
        columns=[
            {'name': 'Song Name', 'id': 'name'},
            {'name': 'Popularity', 'id': 'popularity'}
        ],
        data=artist_data[['name', 'popularity']].to_dict('records'),
        style_cell={'textAlign': 'left', 'fontSize': '18px'}
    )

    return song_table


@app.callback(
    Output('song-chart', 'children'),
    [Input('artist-dropdown', 'value')]
)
def update_song_chart(selected_artist):
    artist_data = data_df[data_df['artists'] == selected_artist]

    data = [
        go.Bar(
            x=artist_data['popularity'],
            y=artist_data['name'],
            orientation='h',
            marker=dict(color='rgba(50, 171, 96, 0.6)',
                        line=dict(color='rgba(50, 171, 96, 1.0)', width=1))
        )
    ]

    layout = dict(
        title='Popularity of Songs by {}'.format(selected_artist),
        xaxis=dict(title='Popularity'),
        yaxis=dict(automargin=True),
        margin=dict(l=140, r=40, t=40, b=30)
    )

    song_chart = dcc.Graph(
        id='song-chart-graph',
        figure=dict(data=data, layout=layout)
    )

    return song_chart


@app.callback(
    [Output('artists_graph', 'figure'),
     Output('artists_tracks_graph', 'figure'),
     Output('songs_graph', 'figure'),
     Output('album_graph', 'figure'),
     Output('popularity_over_time_graph', 'figure'),
     Output('top_albums_graph', 'figure'),
     Output('top_artists_graph', 'figure'),
     Output('top_tracks_graph', 'figure')],
    [Input('artists_graph', 'id')]
)
def update_graph(_):
    # Top 10 Artists by Popularity
    top_artists = data_df.groupby('artists')['popularity'].mean(
    ).reset_index().nlargest(10, 'popularity')
    artists_fig = px.bar(top_artists, x='artists',
                         y='popularity', title='Top 10 Artists by Popularity')

    # Top 10 Artists by Number of Tracks
    top_artists_tracks = data_df.groupby(
        'artists').size().nlargest(10).reset_index(name='Count')
    artists_tracks_fig = px.pie(top_artists_tracks, values='Count',
                                names='artists', title='Top 10 Artists by Number of Tracks')

    # Top 10 Songs by Popularity
    top_songs = data_df.nlargest(10, 'popularity')
    songs_fig = px.scatter(top_songs, x='name', y='popularity',
                           title='Top 10 Songs by Popularity')

    # Best Album by Average Popularity
    album_popularity = data_df.groupby(
        'album_name')['popularity'].mean().reset_index().nlargest(1, 'popularity')
    album_fig = px.scatter(album_popularity, x='album_name',
                           y='popularity', title='Best Album by Average Popularity')

    # Popularity Over Time
    data_df['release_date'] = pd.to_datetime(data_df['release_date'])
    popularity_over_time_fig = px.line(data_df.groupby('release_date')['popularity'].mean(
    ).reset_index(), x='release_date', y='popularity', title='Popularity Over Time')

    # Top Albums by Popularity
    top_albums = data_df.groupby('album_name')['popularity'].mean(
    ).reset_index().nlargest(10, 'popularity')
    top_albums_fig = px.bar(top_albums, x='album_name',
                            y='popularity', title='Top Albums by Popularity')

    # Top Artists by Popularity
    top_artists = data_df.groupby('artists')['popularity'].mean(
    ).reset_index().nlargest(10, 'popularity')
    top_artists_fig = px.scatter(
        top_artists, x='artists', y='popularity', title='Top Artists by Popularity')

    # Top Tracks by Popularity
    top_tracks = data_df.nlargest(10, 'popularity')
    top_tracks_fig = px.bar(top_tracks, x='name',
                            y='popularity', title='Top Tracks by Popularity')

    return (artists_fig, artists_tracks_fig, songs_fig, album_fig,
            popularity_over_time_fig,
            top_albums_fig, top_artists_fig, top_tracks_fig)


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8080, debug=True)
