# spotify_dashboard_final.py

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

# -----------------------------
# MySQL connection
# -----------------------------
DB_USER = "root"
DB_PASSWORD = "rootpassword"
DB_HOST = "localhost"
DB_PORT = "3307"
DB_NAME = "spotify"

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -----------------------------
# Initialize Dash app
# -----------------------------
app = dash.Dash(__name__)
app.title = "Spotify Streaming Dashboard"

# -----------------------------
# Layout
# -----------------------------
app.layout = html.Div(
    style={"backgroundColor": "#0A0A0B", "color": "#ffffff", "fontFamily": "Arial", "padding": "20px"},
    children=[
        html.H1("🎵 Spotify Streaming Dashboard", 
                style={"textAlign": "center", "color": "#00ffd5", "marginBottom": "20px"}),

        # -----------------------------
        # Leaderboard (Vertical bar chart)
        # -----------------------------
        html.Div([
            dcc.Graph(id="top-songs-leaderboard", style={"width": "100%"})
        ], style={"marginBottom": "30px"}),

        # -----------------------------
        # KPI Cards
        # -----------------------------
        html.Div([
            html.Div([
                html.H4("Total Streams", style={"textAlign": "center", "color": "#00ffc5"}),
                html.H2(id="total-streams", style={"textAlign": "center"})
            ], className="card", style={"width": "24%", "display": "inline-block", "backgroundColor": "#292a3a",
                                        "padding": "20px", "margin": "10px", "borderRadius": "10px"}),

            html.Div([
                html.H4("Active Users", style={"textAlign": "center", "color": "#ffcf00"}),
                html.H2(id="active-users", style={"textAlign": "center"})
            ], className="card", style={"width": "24%", "display": "inline-block", "backgroundColor": "#292a3a",
                                        "padding": "20px", "margin": "10px", "borderRadius": "10px"}),

            html.Div([
                html.H4("Top Song", style={"textAlign": "center", "color": "#ff6f61"}),
                html.H2(id="top-song", style={"textAlign": "center"})
            ], className="card", style={"width": "24%", "display": "inline-block", "backgroundColor": "#292a3a",
                                        "padding": "20px", "margin": "10px", "borderRadius": "10px"}),
        ], style={"display": "flex", "justifyContent": "center"}),

        # -----------------------------
        # Line chart trends
        # -----------------------------
        html.Div([
            dcc.Graph(id="top-songs-trend", style={"width": "100%"})
        ]),

        html.Div(id="last-update", style={"textAlign": "center", "marginTop": 10, "color": "#aaa"}),

        dcc.Interval(
            id="interval-component",
            interval=5*1000,  # 5 seconds
            n_intervals=0
        )
    ]
)

# -----------------------------
# Callback
# -----------------------------
@app.callback(
    [Output("top-songs-leaderboard", "figure"),
     Output("top-songs-trend", "figure"),
     Output("total-streams", "children"),
     Output("active-users", "children"),
     Output("top-song", "children"),
     Output("last-update", "children")],
    Input("interval-component", "n_intervals")
)
def update_dashboard(n):
    try:
        # Fetch last 10 minutes data
        query = """
        SELECT 
            FROM_UNIXTIME(FLOOR(timestamp/60)*60) AS minute,
            user_id,
            song,
            COUNT(*) AS streams
        FROM user_streams
        WHERE timestamp >= UNIX_TIMESTAMP(NOW() - INTERVAL 10 MINUTE)
        GROUP BY minute, user_id, song
        ORDER BY minute
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            leaderboard_fig = go.Figure()
            trend_fig = go.Figure()
            total_streams = 0
            active_users = 0
            top_song_name = "N/A"
        else:
            # -----------------------------
            # Leaderboard (Vertical bar chart, animated)
            # -----------------------------
            df_leaderboard = df.groupby("song")["streams"].sum().reset_index().sort_values("streams", ascending=False)
            leaderboard_fig = go.Figure()
            colors = px.colors.qualitative.Dark24

            # Animated bars appear one by one
            for i, row in df_leaderboard.iterrows():
                leaderboard_fig.add_trace(go.Bar(
                    x=[row["song"]],
                    y=[row["streams"]],
                    text=[row["streams"]],
                    textposition='outside',
                    marker_color=colors[i % len(colors)],
                    width=0.3,
                ))
                # Small delay to simulate animation (works in Dash with animation_frame)
                time.sleep(0.1)

            leaderboard_fig.update_layout(
                title="🏆 Top Songs Leaderboard (Last 10 mins)",
                template="plotly_dark",
                xaxis_title="Song",
                yaxis_title="Streams",
                showlegend=False
            )

            # -----------------------------
            # Line chart trends
            # -----------------------------
            df_trend = df.groupby(["minute", "song"])["streams"].sum().reset_index()
            trend_fig = px.line(
                df_trend,
                x="minute",
                y="streams",
                color="song",
                title="Top Songs Trend (Last 10 mins)",
                template="plotly_dark",
                markers=True,
                line_shape='spline',
                color_discrete_sequence=colors
            )
            trend_fig.update_layout(yaxis_title="Streams", xaxis_title="Time", legend_title="Songs")

            # -----------------------------
            # KPI Cards
            # -----------------------------
            total_streams = int(df["streams"].sum())
            active_users = df["user_id"].nunique()
            top_song_name = df_leaderboard.iloc[0]["song"]

        last_update = f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    except Exception as e:
        leaderboard_fig = go.Figure()
        trend_fig = go.Figure()
        total_streams = "Error"
        active_users = "Error"
        top_song_name = "Error"
        last_update = f"Error: {e}"

    return leaderboard_fig, trend_fig, total_streams, active_users, top_song_name, last_update

# -----------------------------
# Run app
# -----------------------------
if __name__ == "__main__":
    app.run(debug=True)
