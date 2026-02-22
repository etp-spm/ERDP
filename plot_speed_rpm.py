"""
Speed vs RPM Plotter
Shows speed vs engine RPM for multiple cars, useful for analyzing gear ratios.

Usage:
    python plot_speed_rpm.py --input D:\26DAY1_Race\laps\by_lap --vehicles 7 71 77
    python plot_speed_rpm.py --input D:\26DAY1_Race\laps\by_lap --vehicles 7 71 77 --laps 5 10 15
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Optional
import pyarrow.parquet as pq
import pandas as pd

from dash import Dash, html, dcc, Output, Input, State
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Configuration
REFRESH_INTERVAL = 5000  # 5 seconds
MAX_LAPS = 20

# Colors for different vehicles
VEHICLE_COLORS = {
    '7': '#FF6B6B',    # Red
    '71': '#4ECDC4',   # Teal
    '77': '#45B7D1',   # Blue
    '8': '#96CEB4',    # Green
    '5': '#FFEAA7',    # Yellow
    '19': '#DDA0DD',   # Plum
    '22': '#FF8C00',   # Orange
    '12': '#98D8C8',   # Mint
}

# Global state
app_state = {
    'input_dir': None,
    'vehicles': [],
    'laps': [],
    'data': {},  # {(vehicle, lap): {'speed': [], 'rpm': []}}
    'paused': False,
}


def init_state(input_dir: str, vehicles: List[str], laps: Optional[List[int]] = None):
    app_state['input_dir'] = Path(input_dir)
    app_state['vehicles'] = vehicles
    app_state['laps'] = laps or []


def get_available_laps(vehicle: str) -> List[int]:
    """Find available lap files for a vehicle"""
    input_dir = app_state['input_dir']
    laps = set()
    
    # Look for Speed files OR location files (Spire cars use location for speed)
    for pattern in [f"Speed_{vehicle}_lap*.parquet", f"location_{vehicle}_lap*.parquet"]:
        for f in input_dir.glob(pattern):
            # Extract lap number from filename
            stem = f.stem
            try:
                lap_str = stem.split('_lap')[-1]
                lap_num = int(lap_str)
                laps.add(lap_num)
            except:
                pass
    
    return sorted(laps)


def read_lap_data(vehicle: str, lap: int):
    """Read Speed and Engine Speed data for a vehicle/lap and compute averages"""
    key = (vehicle, lap)
    input_dir = app_state['input_dir']
    
    # Try Speed file first, fall back to location file (for Spire cars)
    speed_file = input_dir / f"Speed_{vehicle}_lap{lap}.parquet"
    location_file = input_dir / f"location_{vehicle}_lap{lap}.parquet"
    rpm_file = input_dir / f"Engine Speed_{vehicle}_lap{lap}.parquet"
    
    # Determine which file has speed data
    use_location_for_speed = False
    if speed_file.exists():
        speed_source = speed_file
    elif location_file.exists():
        speed_source = location_file
        use_location_for_speed = True
    else:
        return
    
    if not rpm_file.exists():
        return
    
    try:
        # Read speed data
        speed_df = pq.read_table(speed_source).to_pandas()
        rpm_df = pq.read_table(rpm_file).to_pandas()
        
        # Get speed values - from value_numeric or from location JSON
        if use_location_for_speed:
            # Extract 'spd' from value_location JSON
            speeds = []
            for _, row in speed_df.iterrows():
                vl = row.get('value_location')
                if vl and pd.notna(vl):
                    try:
                        data = json.loads(str(vl))
                        spd = data.get('spd')
                        if spd is not None:
                            speeds.append(float(spd))
                    except:
                        pass
            if not speeds:
                return
            avg_speed = sum(speeds) / len(speeds)
            max_speed = max(speeds)
        else:
            avg_speed = speed_df['value_numeric'].mean()
            max_speed = speed_df['value_numeric'].max()
        
        # Get RPM values
        avg_rpm = rpm_df['value_numeric'].mean()
        max_rpm = rpm_df['value_numeric'].max()
        
        # Get lap duration
        lap_duration = speed_df['lap_time'].max()
        
        app_state['data'][key] = {
            'avg_speed': avg_speed,
            'avg_rpm': avg_rpm,
            'max_speed': max_speed,
            'max_rpm': max_rpm,
            'lap_duration': lap_duration
        }
        
    except Exception as e:
        print(f"Error reading data for {vehicle} lap {lap}: {e}")


def create_app():
    app = Dash(__name__)
    
    app.layout = html.Div([
        html.H1("Speed vs RPM Analysis", style={'textAlign': 'center', 'color': '#333'}),
        
        html.Div([
            html.Button('⏸ Pause', id='pause-button', n_clicks=0,
                       style={'marginRight': '10px', 'padding': '10px 20px', 'fontSize': '16px'}),
            html.Button('🔄 Refresh', id='refresh-button', n_clicks=0,
                       style={'marginRight': '10px', 'padding': '10px 20px', 'fontSize': '16px'}),
            html.Button('🗑 Clear', id='clear-button', n_clicks=0,
                       style={'padding': '10px 20px', 'fontSize': '16px'}),
            html.Span(id='pause-status', style={'marginLeft': '20px', 'color': 'red', 'fontWeight': 'bold'}),
        ], style={'textAlign': 'center', 'marginBottom': '20px'}),
        
        # View mode selector
        html.Div([
            html.Label('View Mode:', style={'fontWeight': 'bold', 'marginRight': '10px'}),
            dcc.Dropdown(
                id='view-mode',
                options=[
                    {'label': '📈 Lap Trend (Speed & RPM over laps)', 'value': 'trend'},
                    {'label': '🔗 Path (Speed vs RPM trajectory)', 'value': 'path'},
                    {'label': '📊 Scatter (individual points)', 'value': 'scatter'},
                ],
                value='trend',
                style={'width': '350px', 'display': 'inline-block'}
            ),
        ], style={'textAlign': 'center', 'marginBottom': '20px'}),
        
        # Lap range selector
        html.Div([
            html.Label('Lap Range:', style={'fontWeight': 'bold', 'marginRight': '10px'}),
            html.Div([
                dcc.RangeSlider(
                    id='lap-range-slider',
                    min=1,
                    max=100,
                    step=1,
                    value=[1, 100],
                    marks={i: str(i) for i in range(0, 101, 10)},
                    tooltip={'placement': 'bottom', 'always_visible': True}
                )
            ], style={'width': '80%', 'display': 'inline-block'}),
        ], style={'textAlign': 'center', 'marginBottom': '20px', 'padding': '0 10%'}),
        
        html.Div(id='stats-display', style={'textAlign': 'center', 'marginBottom': '10px', 'color': '#666'}),
        
        dcc.Graph(id='speed-rpm-graph', style={'height': '75vh'}),
        
        dcc.Interval(
            id='interval',
            interval=REFRESH_INTERVAL,
            n_intervals=0
        )
    ])
    
    @app.callback(
        [Output('speed-rpm-graph', 'figure'),
         Output('stats-display', 'children')],
        Input('interval', 'n_intervals'),
        Input('refresh-button', 'n_clicks'),
        Input('lap-range-slider', 'value'),
        Input('view-mode', 'value')
    )
    def update_graph(n_intervals, refresh_clicks, lap_range, view_mode):
        if app_state['paused']:
            # Return current state without updating
            fig = create_figure(lap_range, view_mode)
            return fig, "⏸ PAUSED"
        
        # Load data for all vehicles and laps
        load_all_data()
        
        fig = create_figure(lap_range, view_mode)
        
        total_laps = len(app_state['data'])
        stats = f"Vehicles: {len(app_state['vehicles'])} | Laps: {total_laps} | Showing laps {lap_range[0]}-{lap_range[1]}"
        
        return fig, stats
    
    @app.callback(
        Output('pause-status', 'children'),
        Input('pause-button', 'n_clicks'),
        prevent_initial_call=True
    )
    def toggle_pause(n_clicks):
        app_state['paused'] = not app_state['paused']
        return "⏸ PAUSED - Updates frozen" if app_state['paused'] else ""
    
    @app.callback(
        Output('stats-display', 'children', allow_duplicate=True),
        Input('clear-button', 'n_clicks'),
        prevent_initial_call=True
    )
    def clear_data(n_clicks):
        app_state['data'].clear()
        return "Data cleared"
    
    return app


def load_all_data():
    """Load data for all configured vehicles and laps"""
    for vehicle in app_state['vehicles']:
        # Get available laps if not specified
        if app_state['laps']:
            laps_to_load = app_state['laps']
        else:
            laps_to_load = get_available_laps(vehicle)[:MAX_LAPS]
        
        for lap in laps_to_load:
            key = (vehicle, lap)
            if key not in app_state['data']:
                read_lap_data(vehicle, lap)


def create_figure(lap_range=None, view_mode='trend'):
    """Create the speed vs RPM figure based on view mode"""
    
    vehicles = app_state['vehicles']
    min_lap = lap_range[0] if lap_range else 1
    max_lap = lap_range[1] if lap_range else 100
    
    # Collect data for all vehicles
    all_vehicle_data = {}
    for vehicle in vehicles:
        vehicle_data = [(k, v) for k, v in app_state['data'].items() if k[0] == vehicle]
        vehicle_data.sort(key=lambda x: x[0][1])
        
        laps = []
        avg_speeds = []
        avg_rpms = []
        
        for (veh, lap), data in vehicle_data:
            if lap < min_lap or lap > max_lap:
                continue
            if 'avg_speed' not in data or 'avg_rpm' not in data:
                continue
            
            laps.append(lap)
            avg_speeds.append(data['avg_speed'])
            avg_rpms.append(data['avg_rpm'])
        
        if laps:
            all_vehicle_data[vehicle] = {
                'laps': laps,
                'avg_speeds': avg_speeds,
                'avg_rpms': avg_rpms
            }
    
    if view_mode == 'trend':
        return create_trend_figure(all_vehicle_data)
    elif view_mode == 'path':
        return create_path_figure(all_vehicle_data)
    else:
        return create_scatter_figure(all_vehicle_data)


def create_trend_figure(all_vehicle_data):
    """Create dual-axis line chart showing Speed and RPM trends by lap"""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    for vehicle, data in all_vehicle_data.items():
        color = VEHICLE_COLORS.get(vehicle, '#888888')
        laps = data['laps']
        
        # Speed line (left y-axis)
        fig.add_trace(
            go.Scatter(
                x=laps,
                y=data['avg_speeds'],
                mode='lines+markers',
                name=f"Car {vehicle} Speed",
                line=dict(color=color, width=2),
                marker=dict(size=8, color=color),
                hovertemplate=(
                    f"<b>Car {vehicle}</b><br>"
                    "Lap: %{x}<br>"
                    "Avg Speed: %{y:.1f} mph<br>"
                    "<extra></extra>"
                )
            ),
            secondary_y=False
        )
        
        # RPM line (right y-axis) - dashed
        fig.add_trace(
            go.Scatter(
                x=laps,
                y=data['avg_rpms'],
                mode='lines+markers',
                name=f"Car {vehicle} RPM",
                line=dict(color=color, width=2, dash='dash'),
                marker=dict(size=6, color=color, symbol='diamond'),
                hovertemplate=(
                    f"<b>Car {vehicle}</b><br>"
                    "Lap: %{x}<br>"
                    "Avg RPM: %{y:.0f}<br>"
                    "<extra></extra>"
                )
            ),
            secondary_y=True
        )
    
    fig.update_layout(
        title=dict(text="Speed & RPM Trends by Lap", font=dict(size=20)),
        xaxis=dict(title="Lap Number", gridcolor='rgba(128,128,128,0.2)', dtick=5),
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99, bgcolor='rgba(255,255,255,0.8)'),
        hovermode='x unified'
    )
    
    fig.update_yaxes(title_text="Average Speed (mph)", secondary_y=False, gridcolor='rgba(128,128,128,0.2)')
    fig.update_yaxes(title_text="Average RPM", secondary_y=True, gridcolor='rgba(128,128,128,0.1)')
    
    return fig


def create_path_figure(all_vehicle_data):
    """Create path plot showing Speed vs RPM trajectory connected by lap order"""
    fig = go.Figure()
    
    for vehicle, data in all_vehicle_data.items():
        color = VEHICLE_COLORS.get(vehicle, '#888888')
        laps = data['laps']
        
        # Line connecting points in lap order
        fig.add_trace(go.Scatter(
            x=data['avg_rpms'],
            y=data['avg_speeds'],
            mode='lines+markers+text',
            name=f"Car {vehicle}",
            line=dict(color=color, width=2),
            marker=dict(
                size=10,
                color=laps,  # Color by lap number
                colorscale='Viridis',
                showscale=False,
                line=dict(width=1, color=color)
            ),
            text=[str(lap) for lap in laps],
            textposition='top center',
            textfont=dict(size=9, color=color),
            hovertemplate=(
                f"<b>Car {vehicle}</b><br>"
                "Lap: %{text}<br>"
                "Avg RPM: %{x:.0f}<br>"
                "Avg Speed: %{y:.1f} mph<br>"
                "<extra></extra>"
            )
        ))
        
        # Add arrow annotation for direction (start to end)
        if len(laps) > 1:
            fig.add_annotation(
                x=data['avg_rpms'][0], y=data['avg_speeds'][0],
                text=f"Start (L{laps[0]})",
                showarrow=True, arrowhead=2, arrowcolor=color,
                font=dict(color=color, size=10)
            )
    
    fig.update_layout(
        title=dict(text="Speed vs RPM Path (connected by lap order)", font=dict(size=20)),
        xaxis=dict(title="Average Engine RPM", gridcolor='rgba(128,128,128,0.2)', zeroline=False),
        yaxis=dict(title="Average Speed (mph)", gridcolor='rgba(128,128,128,0.2)', zeroline=False),
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor='rgba(255,255,255,0.8)'),
        hovermode='closest'
    )
    
    return fig


def create_scatter_figure(all_vehicle_data):
    """Create scatter plot (original view)"""
    fig = go.Figure()
    
    for vehicle, data in all_vehicle_data.items():
        color = VEHICLE_COLORS.get(vehicle, '#888888')
        laps = data['laps']
        
        fig.add_trace(go.Scatter(
            x=data['avg_rpms'],
            y=data['avg_speeds'],
            mode='markers+text',
            marker=dict(size=12, color=color, line=dict(width=1, color='white')),
            text=[str(lap) for lap in laps],
            textposition='top center',
            textfont=dict(size=10, color=color),
            name=f"Car {vehicle}",
            hovertemplate=(
                f"<b>Car {vehicle}</b><br>"
                "Lap: %{text}<br>"
                "Avg RPM: %{x:.0f}<br>"
                "Avg Speed: %{y:.1f} mph<br>"
                "<extra></extra>"
            )
        ))
    
    fig.update_layout(
        title=dict(text="Lap Average: Speed vs Engine RPM", font=dict(size=20)),
        xaxis=dict(title="Average Engine RPM", gridcolor='rgba(128,128,128,0.2)', zeroline=False),
        yaxis=dict(title="Average Speed (mph)", gridcolor='rgba(128,128,128,0.2)', zeroline=False),
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor='rgba(255,255,255,0.8)'),
        hovermode='closest'
    )
    
    return fig


def main():
    global REFRESH_INTERVAL, MAX_LAPS
    
    parser = argparse.ArgumentParser(description='Speed vs RPM Plotter')
    parser.add_argument('--input', type=str, default='D:\\26DAY1_Race\\laps\\by_lap',
                        help='Input directory with lap parquet files')
    parser.add_argument('--vehicles', type=str, nargs='+', required=True,
                        help='Vehicle IDs to plot (e.g., 7 71 77)')
    parser.add_argument('--laps', type=int, nargs='+',
                        help='Specific laps to plot (default: all available)')
    parser.add_argument('--interval', type=int, default=REFRESH_INTERVAL,
                        help='Refresh interval in ms')
    parser.add_argument('--max-laps', type=int, default=MAX_LAPS,
                        help='Maximum laps per vehicle')
    parser.add_argument('--port', type=int, default=8052,
                        help='Web server port')
    args = parser.parse_args()
    
    REFRESH_INTERVAL = args.interval
    MAX_LAPS = args.max_laps
    
    init_state(args.input, args.vehicles, args.laps)
    
    print(f"\n{'='*60}")
    print("  Speed vs RPM Dashboard")
    print(f"{'='*60}")
    print(f"  Vehicles: {', '.join(args.vehicles)}")
    print(f"  Source: {args.input}")
    if args.laps:
        print(f"  Laps: {args.laps}")
    print(f"{'='*60}")
    print(f"\n  Open: http://127.0.0.1:{args.port}")
    print("  Press Ctrl+C to stop\n")
    
    app = create_app()
    app.run(port=args.port)


if __name__ == '__main__':
    main()
