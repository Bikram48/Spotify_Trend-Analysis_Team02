import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px

data_df = pd.read_csv("spotifydataset.csv")

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Top 10 Songs and Artists by Popularity"),
    html.Div([
        dcc.Dropdown(
            id='selector',
            options=[
                {'label': 'Top 10 Artists by Popularity', 'value': 'artists'},
                {'label': 'Top 10 Artists by Number of Tracks',
                    'value': 'artists_tracks'},
                {'label': 'Top 10 Songs', 'value': 'songs'},
                {'label': 'Best Album', 'value': 'album'}
            ],
            value='artists'
        ),
        dcc.Graph(id='graph')
    ])
])


@app.callback(
    Output('graph', 'figure'),
    [Input('selector', 'value')]
)
def update_graph(selected_value):
    if selected_value == 'artists':
        top_artists = data_df.groupby('Artist').mean().nlargest(
            10, 'popularity').reset_index()
        fig = px.bar(top_artists, x='Artist', y='popularity',
                     title='Top 10 Artists by Popularity')
    elif selected_value == 'artists_tracks':
        top_artists = data_df.groupby('Artist').size().nlargest(
            10).reset_index(name='Count')
        fig = px.bar(top_artists, x='Artist', y='Count',
                     title='Top 10 Artists by Number of Tracks')
    elif selected_value == 'songs':
        top_songs = data_df.nlargest(10, 'popularity')
        fig = px.bar(top_songs, x='Track Name', y='popularity',
                     title='Top 10 Songs by Popularity')
    else:
        album_popularity = data_df.groupby(
            'Album')['popularity'].mean().reset_index()
        best_album = album_popularity.nlargest(1, 'popularity')
        fig = px.bar(best_album, x='Album', y='popularity',
                     title='Best Album by Average Popularity')

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
