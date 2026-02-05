"""
Weather Dashboard - CLI Application
Connects to HDFS, loads LSTM model, and serves predictions
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from hdfs import InsecureClient
from tensorflow import keras
from tensorflow.keras import losses, metrics
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherDashboard:
    """Main dashboard class for weather predictions"""
    
    def __init__(self, hdfs_url, model_path):
        """
        Initialize dashboard with HDFS connection and model
        
        Args:
            hdfs_url: HDFS NameNode URL (e.g., 'http://hdfs-namenode:9870')
            model_path: Path to the trained .h5 model file
        """
        self.hdfs_url = hdfs_url
        self.model_path = model_path
        self.model = None
        self.hdfs_client = None
        self.SEQUENCE_LENGTH = 24
        
        logger.info(f"Initializing Weather Dashboard...")
        logger.info(f"HDFS URL: {hdfs_url}")
        logger.info(f"Model path: {model_path}")
        
        self._connect_hdfs()
        self._load_model()
    
    def _connect_hdfs(self):
        """Establish connection to HDFS"""
        try:
            # Extract host and port from URL
            url_parts = self.hdfs_url.replace('http://', '').replace('https://', '')
            
            self.hdfs_client = InsecureClient(self.hdfs_url, user='root')
            
            # Test connection
            self.hdfs_client.status('/')
            logger.info("✅ Successfully connected to HDFS")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to HDFS: {e}")
            raise
    
    def _load_model(self):
        """Load the trained LSTM model with proper custom objects"""
        try:
            if not os.path.exists(self.model_path):
                raise FileNotFoundError(f"Model file not found: {self.model_path}")
            
            # Define custom objects for Keras 3.x compatibility
            custom_objects = {
                'mse': losses.MeanSquaredError(),
                'mae': metrics.MeanAbsoluteError()
            }
            
            # Load model with custom objects
            self.model = keras.models.load_model(
                self.model_path,
                custom_objects=custom_objects,
                compile=False  # Don't compile during load
            )
            
            # Recompile with proper objects
            self.model.compile(
                optimizer=keras.optimizers.Adam(learning_rate=0.001),
                loss=losses.MeanSquaredError(),
                metrics=[metrics.MeanAbsoluteError()]
            )
            
            logger.info(f"✅ Successfully loaded model from {self.model_path}")
            logger.info(f"Model input shape: {self.model.input_shape}")
            logger.info(f"Model output shape: {self.model.output_shape}")
            
        except Exception as e:
            logger.error(f"❌ Failed to load model: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            raise
    
    def get_available_locations(self):
        """Get list of available locations from HDFS"""
        try:
            # List partitions in the raw data directory
            raw_path = '/weather/raw'
            
            locations = []
            for item in self.hdfs_client.list(raw_path):
                if item.startswith('location='):
                    location = item.replace('location=', '')
                    locations.append(location)
            
            logger.info(f"Found {len(locations)} locations: {locations}")
            return sorted(locations)
            
        except Exception as e:
            logger.error(f"Error getting locations: {e}")
            return []
    
    def fetch_recent_data(self, location, hours=48):
        """
        Fetch recent weather data for a location from HDFS
        
        Args:
            location: City name
            hours: Number of recent hours to fetch
            
        Returns:
            pandas DataFrame with recent data
        """
        try:
            # Path to location partition
            partition_path = f'/weather/raw/location={location}'
            
            # Read parquet files
            parquet_files = []
            for file in self.hdfs_client.list(partition_path):
                if file.endswith('.parquet'):
                    file_path = f'{partition_path}/{file}'
                    parquet_files.append(file_path)
            
            if not parquet_files:
                logger.warning(f"No parquet files found for {location}")
                return pd.DataFrame()
            
            # Read all parquet files
            dfs = []
            for file_path in parquet_files:
                with self.hdfs_client.read(file_path) as reader:
                    df = pd.read_parquet(reader)
                    dfs.append(df)
            
            if not dfs:
                return pd.DataFrame()
            
            # Combine all dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Convert timestamp to datetime
            combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
            
            # Sort by timestamp
            combined_df = combined_df.sort_values('timestamp')
            
            # Filter to recent hours
            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent_df = combined_df[combined_df['timestamp'] >= cutoff_time]
            
            logger.info(f"Fetched {len(recent_df)} records for {location}")
            return recent_df
            
        except Exception as e:
            logger.error(f"Error fetching data for {location}: {e}")
            return pd.DataFrame()
    
    def prepare_features(self, df):
        """
        Prepare features for model prediction
        
        Args:
            df: DataFrame with weather data
            
        Returns:
            numpy array with prepared features
        """
        if df.empty or len(df) < self.SEQUENCE_LENGTH:
            logger.warning(f"Not enough data for prediction (need {self.SEQUENCE_LENGTH} records)")
            return None
        
        # Create time-based features
        df = df.copy()
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
        
        # Cyclical encoding for time features
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
        df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        
        # Select features (adjust based on your model training)
        feature_cols = [
            'temperature', 'humidity', 'wind_speed',
            'hour_sin', 'hour_cos', 
            'day_sin', 'day_cos',
            'month_sin', 'month_cos'
        ]
        
        features = df[feature_cols].values
        
        return features
    
    def create_sequence(self, features):
        """
        Create sequence for LSTM input
        
        Args:
            features: numpy array of features
            
        Returns:
            numpy array shaped for LSTM input (1, SEQUENCE_LENGTH, n_features)
        """
        if len(features) < self.SEQUENCE_LENGTH:
            return None
        
        # Take the most recent SEQUENCE_LENGTH records
        sequence = features[-self.SEQUENCE_LENGTH:]
        
        # Reshape for LSTM: (batch_size, time_steps, features)
        sequence = sequence.reshape(1, self.SEQUENCE_LENGTH, -1)
        
        return sequence
    
    def predict_temperature(self, location):
        """
        Predict next temperature for a location
        
        Args:
            location: City name
            
        Returns:
            dict with prediction results
        """
        try:
            # Fetch recent data
            logger.info(f"Fetching recent data for {location}...")
            df = self.fetch_recent_data(location, hours=48)
            
            if df.empty:
                return {
                    'status': 'error',
                    'message': f'No data available for {location}'
                }
            
            # Prepare features
            logger.info("Preparing features...")
            features = self.prepare_features(df)
            
            if features is None:
                return {
                    'status': 'error',
                    'message': f'Not enough data for prediction (need {self.SEQUENCE_LENGTH} records)'
                }
            
            # Create sequence
            sequence = self.create_sequence(features)
            
            if sequence is None:
                return {
                    'status': 'error',
                    'message': 'Failed to create sequence'
                }
            
            # Make prediction
            logger.info("Making prediction...")
            prediction = self.model.predict(sequence, verbose=0)
            predicted_temp = float(prediction[0][0])
            
            # Get current temperature
            current_temp = float(df.iloc[-1]['temperature'])
            
            # Calculate change
            temp_change = predicted_temp - current_temp
            
            return {
                'status': 'success',
                'location': location,
                'current_temperature': current_temp,
                'predicted_temperature': predicted_temp,
                'temperature_change': temp_change,
                'current_humidity': float(df.iloc[-1]['humidity']),
                'current_wind_speed': float(df.iloc[-1]['wind_speed']),
                'prediction_time': datetime.now().isoformat(),
                'data_points': len(df)
            }
            
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_statistics(self, location, hours=24):
        """
        Get statistical summary for a location
        
        Args:
            location: City name
            hours: Number of hours to analyze
            
        Returns:
            dict with statistics
        """
        try:
            df = self.fetch_recent_data(location, hours=hours)
            
            if df.empty:
                return None
            
            stats = {
                'location': location,
                'time_period_hours': hours,
                'record_count': len(df),
                'temperature': {
                    'mean': float(df['temperature'].mean()),
                    'min': float(df['temperature'].min()),
                    'max': float(df['temperature'].max()),
                    'std': float(df['temperature'].std())
                },
                'humidity': {
                    'mean': float(df['humidity'].mean()),
                    'min': float(df['humidity'].min()),
                    'max': float(df['humidity'].max())
                },
                'wind_speed': {
                    'mean': float(df['wind_speed'].mean()),
                    'min': float(df['wind_speed'].min()),
                    'max': float(df['wind_speed'].max())
                }
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return None


def run_cli():
    """Run the CLI dashboard"""
    
    # Get configuration from environment
    hdfs_url = os.getenv('HDFS_URL', 'http://hdfs-namenode:9870')
    model_path = os.getenv('MODEL_PATH', '/app/models/weather_lstm_model_fixed.h5')
    
    print("\n" + "="*60)
    print("🌤️  WEATHER PREDICTION DASHBOARD (CLI)")
    print("="*60 + "\n")
    
    try:
        # Initialize dashboard
        dashboard = WeatherDashboard(hdfs_url, model_path)
        
        while True:
            print("\n" + "-"*60)
            print("MENU:")
            print("-"*60)
            print("1. List available locations")
            print("2. Predict temperature for a location")
            print("3. View statistics for a location")
            print("4. Exit")
            print("-"*60)
            
            choice = input("\nEnter your choice (1-4): ").strip()
            
            if choice == '1':
                locations = dashboard.get_available_locations()
                print(f"\n📍 Available Locations ({len(locations)}):")
                for i, loc in enumerate(locations, 1):
                    print(f"  {i}. {loc}")
            
            elif choice == '2':
                location = input("\nEnter location name: ").strip()
                print(f"\n🔮 Predicting temperature for {location}...")
                
                result = dashboard.predict_temperature(location)
                
                if result['status'] == 'success':
                    print("\n" + "="*60)
                    print(f"📊 PREDICTION RESULTS FOR {result['location'].upper()}")
                    print("="*60)
                    print(f"Current Temperature:    {result['current_temperature']:.2f}°C")
                    print(f"Predicted Temperature:  {result['predicted_temperature']:.2f}°C")
                    print(f"Expected Change:        {result['temperature_change']:+.2f}°C")
                    print(f"Current Humidity:       {result['current_humidity']:.1f}%")
                    print(f"Current Wind Speed:     {result['current_wind_speed']:.2f} m/s")
                    print(f"Data Points Used:       {result['data_points']}")
                    print("="*60)
                else:
                    print(f"\n❌ Error: {result['message']}")
            
            elif choice == '3':
                location = input("\nEnter location name: ").strip()
                hours = input("Enter number of hours (default 24): ").strip()
                hours = int(hours) if hours else 24
                
                print(f"\n📈 Fetching statistics for {location}...")
                stats = dashboard.get_statistics(location, hours)
                
                if stats:
                    print("\n" + "="*60)
                    print(f"📊 STATISTICS FOR {stats['location'].upper()}")
                    print(f"Time Period: Last {stats['time_period_hours']} hours")
                    print(f"Records: {stats['record_count']}")
                    print("="*60)
                    print("\n🌡️  TEMPERATURE:")
                    print(f"  Mean:  {stats['temperature']['mean']:.2f}°C")
                    print(f"  Min:   {stats['temperature']['min']:.2f}°C")
                    print(f"  Max:   {stats['temperature']['max']:.2f}°C")
                    print(f"  Std:   {stats['temperature']['std']:.2f}°C")
                    print("\n💧 HUMIDITY:")
                    print(f"  Mean:  {stats['humidity']['mean']:.1f}%")
                    print(f"  Min:   {stats['humidity']['min']:.1f}%")
                    print(f"  Max:   {stats['humidity']['max']:.1f}%")
                    print("\n💨 WIND SPEED:")
                    print(f"  Mean:  {stats['wind_speed']['mean']:.2f} m/s")
                    print(f"  Min:   {stats['wind_speed']['min']:.2f} m/s")
                    print(f"  Max:   {stats['wind_speed']['max']:.2f} m/s")
                    print("="*60)
                else:
                    print(f"\n❌ Could not fetch statistics for {location}")
            
            elif choice == '4':
                print("\n👋 Goodbye!")
                break
            
            else:
                print("\n❌ Invalid choice. Please try again.")
    
    except KeyboardInterrupt:
        print("\n\n👋 Dashboard stopped by user")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run_cli()
