"""
scripts/transform_data.py
Data transformation and cleaning script
"""

import pandas as pd
import os
from datetime import datetime

class F1DataTransformer:
    """Transform and clean F1 data for database loading"""
    
    def __init__(self, raw_data_path='data/raw/', processed_data_path='data/processed/'):
        self.raw_path = raw_data_path
        self.processed_path = processed_data_path
        
        # Create directories if they don't exist
        os.makedirs(raw_data_path, exist_ok=True)
        os.makedirs(processed_data_path, exist_ok=True)
    
    def transform_circuits(self):
        """Transform circuits data"""
        if not os.path.exists(f'{self.raw_path}circuits.csv') or os.path.getsize(f'{self.raw_path}circuits.csv') < 10:
            print("⚠ Warning: circuits.csv is empty or missing")
            pd.DataFrame().to_csv(f'{self.processed_path}circuits_clean.csv', index=False)
            return pd.DataFrame()
            
        df = pd.read_csv(f'{self.raw_path}circuits.csv')
        
        # Create numeric circuit_id from circuit_ref
        df['circuit_id'] = range(1, len(df) + 1)
        
        # Handle missing altitude values
        df['altitude'] = df['altitude'].fillna(0)
        
        # Reorder columns
        df = df[['circuit_id', 'circuit_ref', 'circuit_name', 'location', 
                 'country', 'lat', 'lng', 'altitude', 'url']]
        
        df.to_csv(f'{self.processed_path}circuits_clean.csv', index=False)
        print(f"✓ Transformed {len(df)} circuits")
        return df
    
    def transform_drivers(self):
        """Transform drivers data"""
        df = pd.read_csv(f'{self.raw_path}drivers.csv')
        
        # Create numeric driver_id from driver_ref (use existing driver_id if available)
        if 'driver_id' not in df.columns:
            df['driver_id'] = range(1, len(df) + 1)
        
        # Convert DOB to proper date format
        if 'dob' in df.columns:
            df['dob'] = pd.to_datetime(df['dob'], errors='coerce')
        
        # Handle missing driver numbers
        if 'driver_number' in df.columns:
            df['driver_number'] = df['driver_number'].fillna(0).astype(int)
        else:
            df['driver_number'] = 0
        
        # Ensure all required columns exist
        required_cols = ['driver_id', 'driver_ref', 'driver_number', 'code', 
                        'forename', 'surname', 'dob', 'nationality', 'url']
        for col in required_cols:
            if col not in df.columns:
                if col == 'code':
                    df[col] = ''
                elif col == 'url':
                    df[col] = ''
                else:
                    df[col] = None
        
        # Reorder columns
        df = df[required_cols]
        
        df.to_csv(f'{self.processed_path}drivers_clean.csv', index=False)
        print(f"✓ Transformed {len(df)} drivers")
        return df
    
    def transform_races(self):
        """Transform races data"""
        df = pd.read_csv(f'{self.raw_path}races.csv')
        
        # Convert dates and times
        if 'race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'], errors='coerce')
        
        # Handle missing race times
        if 'race_time' in df.columns:
            df['race_time'] = df['race_time'].fillna('00:00:00')
        else:
            df['race_time'] = '00:00:00'
        
        # Map circuit_ref to circuit_id
        if 'circuit_ref' in df.columns:
            try:
                circuits_df = pd.read_csv(f'{self.raw_path}circuits.csv')
                if 'circuit_id' not in circuits_df.columns:
                    circuits_df['circuit_id'] = range(1, len(circuits_df) + 1)
                circuit_map = dict(zip(circuits_df['circuit_ref'], circuits_df['circuit_id']))
                df['circuit_id'] = df['circuit_ref'].map(circuit_map)
                df['circuit_id'] = df['circuit_id'].fillna(0).astype(int)
            except:
                print("⚠ Warning: Could not map circuit_ref to circuit_id")
                df['circuit_id'] = 0
        
        # Ensure race_id is integer
        if 'race_id' in df.columns:
            df['race_id'] = df['race_id'].astype(int)
        else:
            df['race_id'] = (df['year'].astype(str) + df['round'].astype(str).str.zfill(2)).astype(int)
        
        df.to_csv(f'{self.processed_path}races_clean.csv', index=False)
        print(f"✓ Transformed {len(df)} races")
        return df
    
    def transform_results(self):
        """Transform results data"""
        if not os.path.exists(f'{self.raw_path}results.csv') or os.path.getsize(f'{self.raw_path}results.csv') < 10:
            print("⚠ Warning: results.csv is empty or missing")
            pd.DataFrame().to_csv(f'{self.processed_path}results_clean.csv', index=False)
            return pd.DataFrame()
            
        df = pd.read_csv(f'{self.raw_path}results.csv')
        
        # Map driver_ref and constructor_ref to IDs
        try:
            drivers_df = pd.read_csv(f'{self.raw_path}drivers.csv')
            if 'driver_id' not in drivers_df.columns:
                drivers_df['driver_id'] = range(1, len(drivers_df) + 1)
            driver_map = dict(zip(drivers_df['driver_ref'], drivers_df['driver_id']))
            df['driver_id'] = df['driver_ref'].map(driver_map)
        except:
            print("⚠ Warning: Could not map driver_ref to driver_id")
            df['driver_id'] = 0
        
        try:
            constructors_df = pd.read_csv(f'{self.raw_path}constructors.csv')
            constructor_map = dict(zip(constructors_df['constructor_ref'], constructors_df['constructor_id']))
            df['constructor_id'] = df['constructor_ref'].map(constructor_map)
        except:
            print("⚠ Warning: Could not map constructor_ref to constructor_id")
            df['constructor_id'] = 0
        
        # Convert position to numeric (handle non-numeric positions)
        if 'position' in df.columns:
            df['position'] = pd.to_numeric(df['position'], errors='coerce')
        
        # Fill missing values
        numeric_cols = ['points', 'laps', 'grid', 'number', 'fastest_lap', 'fastest_lap_rank']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        
        # Handle milliseconds
        if 'milliseconds' in df.columns:
            df['milliseconds'] = pd.to_numeric(df['milliseconds'], errors='coerce').fillna(0).astype(int)
        else:
            df['milliseconds'] = 0
        
        # Create status_id based on status text
        status_map = {
            'Finished': 1,
            '+1 Lap': 11,
            '+2 Laps': 12,
            '+3 Laps': 13,
            'Retired': 14,
            'Disqualified': 2,
            'Accident': 3,
            'Collision': 4,
            'Engine': 5,
        }
        if 'status' in df.columns:
            df['status_id'] = df['status'].map(status_map)
            df['status_id'] = df['status_id'].fillna(14)  # Default to 'Retired'
        else:
            df['status_id'] = 1  # Default to 'Finished'
        
        # Handle position_order
        if 'position_order' not in df.columns:
            df['position_order'] = df['position'].fillna(999)
        df['position_order'] = pd.to_numeric(df['position_order'], errors='coerce').fillna(999).astype(int)
        
        df.to_csv(f'{self.processed_path}results_clean.csv', index=False)
        print(f"✓ Transformed {len(df)} results")
        return df
    
    def transform_qualifying(self):
        """Transform qualifying data"""
        df = pd.read_csv(f'{self.raw_path}qualifying.csv')
        
        # Map driver_ref and constructor_ref to IDs
        try:
            drivers_df = pd.read_csv(f'{self.raw_path}drivers.csv')
            if 'driver_id' not in drivers_df.columns:
                drivers_df['driver_id'] = range(1, len(drivers_df) + 1)
            driver_map = dict(zip(drivers_df['driver_ref'], drivers_df['driver_id']))
            df['driver_id'] = df['driver_ref'].map(driver_map)
        except:
            print("⚠ Warning: Could not map driver_ref to driver_id")
            df['driver_id'] = 0
        
        try:
            constructors_df = pd.read_csv(f'{self.raw_path}constructors.csv')
            constructor_map = dict(zip(constructors_df['constructor_ref'], constructors_df['constructor_id']))
            df['constructor_id'] = df['constructor_ref'].map(constructor_map)
        except:
            print("⚠ Warning: Could not map constructor_ref to constructor_id")
            df['constructor_id'] = 0
        
        # Handle missing Q2 and Q3 times
        for col in ['q1', 'q2', 'q3']:
            if col in df.columns:
                df[col] = df[col].fillna('')
            else:
                df[col] = ''
        
        # Ensure numeric columns
        for col in ['position', 'number']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        df.to_csv(f'{self.processed_path}qualifying_clean.csv', index=False)
        print(f"✓ Transformed {len(df)} qualifying results")
        return df
    
    def transform_pit_stops(self):
        """Transform pit stops data"""
        if not os.path.exists(f'{self.raw_path}pit_stops.csv') or os.path.getsize(f'{self.raw_path}pit_stops.csv') < 10:
            print("⚠ Warning: pit_stops.csv is empty or missing")
            pd.DataFrame().to_csv(f'{self.processed_path}pit_stops_clean.csv', index=False)
            return pd.DataFrame()
            
        df = pd.read_csv(f'{self.raw_path}pit_stops.csv')
        
        # Map driver_ref to driver_id
        try:
            drivers_df = pd.read_csv(f'{self.raw_path}drivers.csv')
            if 'driver_id' not in drivers_df.columns:
                drivers_df['driver_id'] = range(1, len(drivers_df) + 1)
            driver_map = dict(zip(drivers_df['driver_ref'], drivers_df['driver_id']))
            df['driver_id'] = df['driver_ref'].map(driver_map)
        except:
            print("⚠ Warning: Could not map driver_ref to driver_id")
            df['driver_id'] = 0
        
        # Convert time to proper format (keep as string for now)
        if 'time_of_day' in df.columns:
            df['time_of_day'] = df['time_of_day'].fillna('00:00:00')
        else:
            df['time_of_day'] = '00:00:00'
        
        # Calculate milliseconds from duration if needed
        if 'milliseconds' not in df.columns or df['milliseconds'].isna().all():
            if 'duration' in df.columns:
                df['milliseconds'] = pd.to_numeric(df['duration'], errors='coerce') * 1000
            else:
                df['milliseconds'] = 0
        
        # Ensure milliseconds is numeric
        df['milliseconds'] = pd.to_numeric(df['milliseconds'], errors='coerce').fillna(0).astype(int)
        
        df.to_csv(f'{self.processed_path}pit_stops_clean.csv', index=False)
        print(f"✓ Transformed {len(df)} pit stops")
        return df
    
    def transform_standings(self):
        """Transform constructor and driver standings"""
        # Constructor standings
        if not os.path.exists(f'{self.raw_path}constructor_standings.csv') or os.path.getsize(f'{self.raw_path}constructor_standings.csv') < 10:
            print("⚠ Warning: constructor_standings.csv is empty or missing")
            df_const = pd.DataFrame()
        else:
            df_const = pd.read_csv(f'{self.raw_path}constructor_standings.csv')
        
        if not df_const.empty:
            # Map constructor_ref to constructor_id
            try:
                constructors_df = pd.read_csv(f'{self.raw_path}constructors.csv')
                constructor_map = dict(zip(constructors_df['constructor_ref'], constructors_df['constructor_id']))
                df_const['constructor_id'] = df_const['constructor_ref'].map(constructor_map)
            except:
                print("⚠ Warning: Could not map constructor_ref to constructor_id")
                df_const['constructor_id'] = 0
            
            df_const['points'] = df_const['points'].fillna(0)
            df_const['wins'] = df_const['wins'].fillna(0)
            df_const.to_csv(f'{self.processed_path}constructor_standings_clean.csv', index=False)
            print(f"✓ Transformed {len(df_const)} constructor standings")
        else:
            print("⚠ Warning: Skipping constructor standings transformation (empty)")
        
        # Driver standings
        if not os.path.exists(f'{self.raw_path}driver_standings.csv') or os.path.getsize(f'{self.raw_path}driver_standings.csv') < 10:
            print("⚠ Warning: driver_standings.csv is empty or missing")
            df_driver = pd.DataFrame()
        else:
            df_driver = pd.read_csv(f'{self.raw_path}driver_standings.csv')
        
        if not df_driver.empty:
            # Map driver_ref to driver_id
            try:
                drivers_df = pd.read_csv(f'{self.raw_path}drivers.csv')
                if 'driver_id' not in drivers_df.columns:
                    drivers_df['driver_id'] = range(1, len(drivers_df) + 1)
                driver_map = dict(zip(drivers_df['driver_ref'], drivers_df['driver_id']))
                df_driver['driver_id'] = df_driver['driver_ref'].map(driver_map)
            except:
                print("⚠ Warning: Could not map driver_ref to driver_id")
                df_driver['driver_id'] = 0
            
            df_driver['points'] = df_driver['points'].fillna(0)
            df_driver['wins'] = df_driver['wins'].fillna(0)
            df_driver.to_csv(f'{self.processed_path}driver_standings_clean.csv', index=False)
            print(f"✓ Transformed {len(df_driver)} driver standings")
        else:
            print("⚠ Warning: Skipping driver standings transformation (empty)")
        
        return df_const, df_driver
    
    def transform_all(self):
        """Run all transformations"""
        print("Starting data transformation...")
        print("=" * 60)
        
        try:
            self.transform_circuits()
            self.transform_drivers()
            self.transform_races()
            self.transform_results()
            self.transform_qualifying()
            self.transform_pit_stops()
            self.transform_standings()
            
            print("=" * 60)
            print("✓ All transformations completed successfully!")
            print(f"Cleaned data saved to: {self.processed_path}")
            print("=" * 60)
            
        except Exception as e:
            print(f"❌ Error during transformation: {e}")
            import traceback
            traceback.print_exc()
            raise

def main():
    transformer = F1DataTransformer()
    transformer.transform_all()

if __name__ == "__main__":
    main()

