"""
Weather Dashboard - Streamlit Web UI
Provides interactive web interface for weather predictions
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os
import sys

# Import the dashboard logic
from dashboard_app import WeatherDashboard

# Page configuration
st.set_page_config(
    page_title="Weather Prediction Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def initialize_dashboard():
    """Initialize the dashboard (cached to avoid reconnecting)"""
    hdfs_url = os.getenv('HDFS_URL', 'http://hdfs-namenode:9870')
    model_path = os.getenv('MODEL_PATH', '/app/models/weather_lstm_model.h5')
    
    return WeatherDashboard(hdfs_url, model_path)


def create_temperature_gauge(current_temp, predicted_temp):
    """Create a gauge chart for temperature comparison"""
    
    fig = go.Figure()
    
    # Current temperature gauge
    fig.add_trace(go.Indicator(
        mode="gauge+number+delta",
        value=predicted_temp,
        title={'text': "Predicted Temperature (°C)", 'font': {'size': 20}},
        delta={'reference': current_temp, 'suffix': "°C"},
        gauge={
            'axis': {'range': [-20, 50]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [-20, 0], 'color': "lightblue"},
                {'range': [0, 15], 'color': "lightgreen"},
                {'range': [15, 30], 'color': "yellow"},
                {'range': [30, 50], 'color': "orange"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': current_temp
            }
        }
    ))
    
    fig.update_layout(height=300)
    
    return fig


def create_metrics_chart(result):
    """Create a bar chart for weather metrics"""
    
    metrics = {
        'Current Temp (°C)': result['current_temperature'],
        'Predicted Temp (°C)': result['predicted_temperature'],
        'Humidity (%)': result['current_humidity'],
        'Wind Speed (m/s)': result['current_wind_speed']
    }
    
    fig = go.Figure(data=[
        go.Bar(
            x=list(metrics.keys()),
            y=list(metrics.values()),
            marker_color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
        )
    ])
    
    fig.update_layout(
        title="Weather Metrics Overview",
        xaxis_title="Metric",
        yaxis_title="Value",
        height=400
    )
    
    return fig


def create_trend_chart(df, metric='temperature'):
    """Create a line chart for historical trends"""
    
    if df.empty:
        return None
    
    fig = px.line(
        df,
        x='timestamp',
        y=metric,
        title=f'{metric.capitalize()} Trend',
        labels={'timestamp': 'Time', metric: metric.capitalize()}
    )
    
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title=metric.capitalize(),
        hovermode='x unified',
        height=400
    )
    
    return fig


def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown('<div class="main-header">🌤️ Weather Prediction Dashboard</div>', unsafe_allow_html=True)
    
    # Initialize dashboard
    try:
        with st.spinner("Initializing dashboard..."):
            dashboard = initialize_dashboard()
        st.success("✅ Dashboard initialized successfully!")
    except Exception as e:
        st.error(f"❌ Failed to initialize dashboard: {e}")
        st.stop()
    
    # Sidebar
    st.sidebar.title("🎛️ Controls")
    
    # Get available locations
    with st.spinner("Loading locations..."):
        locations = dashboard.get_available_locations()
    
    if not locations:
        st.error("❌ No locations found in HDFS. Please check your data pipeline.")
        st.stop()
    
    # Location selector
    selected_location = st.sidebar.selectbox(
        "📍 Select Location",
        locations,
        index=0
    )
    
    # Time range for statistics
    hours_range = st.sidebar.slider(
        "⏰ Time Range (hours)",
        min_value=6,
        max_value=72,
        value=24,
        step=6
    )
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data", type="primary"):
        st.cache_resource.clear()
        st.rerun()
    
    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Prediction", "📈 Statistics", "📋 Raw Data", "📉 Trends"])
    
    # TAB 1: PREDICTION
    with tab1:
        st.header(f"Temperature Prediction for {selected_location}")
        
        if st.button("🔮 Generate Prediction", type="primary", key="predict_btn"):
            with st.spinner(f"Generating prediction for {selected_location}..."):
                result = dashboard.predict_temperature(selected_location)
            
            if result['status'] == 'success':
                st.markdown('<div class="success-box">✅ Prediction generated successfully!</div>', unsafe_allow_html=True)
                
                # Display metrics in columns
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric(
                        "Current Temperature",
                        f"{result['current_temperature']:.2f}°C"
                    )
                
                with col2:
                    st.metric(
                        "Predicted Temperature",
                        f"{result['predicted_temperature']:.2f}°C",
                        delta=f"{result['temperature_change']:+.2f}°C"
                    )
                
                with col3:
                    st.metric(
                        "Humidity",
                        f"{result['current_humidity']:.1f}%"
                    )
                
                with col4:
                    st.metric(
                        "Wind Speed",
                        f"{result['current_wind_speed']:.2f} m/s"
                    )
                
                # Visualizations
                col5, col6 = st.columns(2)
                
                with col5:
                    gauge_fig = create_temperature_gauge(
                        result['current_temperature'],
                        result['predicted_temperature']
                    )
                    st.plotly_chart(gauge_fig, use_container_width=True)
                
                with col6:
                    metrics_fig = create_metrics_chart(result)
                    st.plotly_chart(metrics_fig, use_container_width=True)
                
                # Additional info
                st.info(f"📊 Data points used: {result['data_points']} | ⏰ Prediction time: {result['prediction_time']}")
            
            else:
                st.markdown(f'<div class="error-box">❌ Error: {result["message"]}</div>', unsafe_allow_html=True)
    
    # TAB 2: STATISTICS
    with tab2:
        st.header(f"Statistics for {selected_location}")
        
        with st.spinner(f"Loading statistics..."):
            stats = dashboard.get_statistics(selected_location, hours=hours_range)
        
        if stats:
            st.info(f"📊 Showing statistics for the last {stats['time_period_hours']} hours ({stats['record_count']} records)")
            
            # Temperature statistics
            st.subheader("🌡️ Temperature")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Mean", f"{stats['temperature']['mean']:.2f}°C")
            col2.metric("Min", f"{stats['temperature']['min']:.2f}°C")
            col3.metric("Max", f"{stats['temperature']['max']:.2f}°C")
            col4.metric("Std Dev", f"{stats['temperature']['std']:.2f}°C")
            
            # Humidity statistics
            st.subheader("💧 Humidity")
            col5, col6, col7 = st.columns(3)
            col5.metric("Mean", f"{stats['humidity']['mean']:.1f}%")
            col6.metric("Min", f"{stats['humidity']['min']:.1f}%")
            col7.metric("Max", f"{stats['humidity']['max']:.1f}%")
            
            # Wind speed statistics
            st.subheader("💨 Wind Speed")
            col8, col9, col10 = st.columns(3)
            col8.metric("Mean", f"{stats['wind_speed']['mean']:.2f} m/s")
            col9.metric("Min", f"{stats['wind_speed']['min']:.2f} m/s")
            col10.metric("Max", f"{stats['wind_speed']['max']:.2f} m/s")
        else:
            st.error(f"❌ Could not load statistics for {selected_location}")
    
    # TAB 3: RAW DATA
    with tab3:
        st.header(f"Raw Data for {selected_location}")
        
        with st.spinner("Loading raw data..."):
            df = dashboard.fetch_recent_data(selected_location, hours=hours_range)
        
        if not df.empty:
            st.success(f"✅ Loaded {len(df)} records")
            
            # Display dataframe
            st.dataframe(
                df.sort_values('timestamp', ascending=False),
                use_container_width=True,
                height=400
            )
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"{selected_location}_weather_data.csv",
                mime="text/csv"
            )
        else:
            st.warning(f"⚠️ No data available for {selected_location}")
    
    # TAB 4: TRENDS
    with tab4:
        st.header(f"Trends for {selected_location}")
        
        with st.spinner("Loading trend data..."):
            df = dashboard.fetch_recent_data(selected_location, hours=hours_range)
        
        if not df.empty:
            # Temperature trend
            temp_fig = create_trend_chart(df, 'temperature')
            if temp_fig:
                st.plotly_chart(temp_fig, use_container_width=True)
            
            # Humidity trend
            humidity_fig = create_trend_chart(df, 'humidity')
            if humidity_fig:
                st.plotly_chart(humidity_fig, use_container_width=True)
            
            # Wind speed trend
            wind_fig = create_trend_chart(df, 'wind_speed')
            if wind_fig:
                st.plotly_chart(wind_fig, use_container_width=True)
        else:
            st.warning(f"⚠️ No data available for trends")
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ About")
    st.sidebar.info(
        "This dashboard provides real-time weather predictions using an LSTM model "
        "trained on historical weather data. Data is stored in HDFS and processed "
        "using Apache Spark."
    )


if __name__ == "__main__":
    main()
