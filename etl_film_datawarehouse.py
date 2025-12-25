"""
ETL Script for Film DataWarehouse
"""

import pandas as pd
import psycopg2
from psycopg2 import extras
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# DATABASE CONNECTION CONFIGURATION

DB_CONFIG = {
    'host': 'localhost',
    'database': 'MovieDW',  
    'user': 'postgres',     
    'password': 'admin123',
    'port': 5432
}

# FILE PATH

EXCEL_FILE = r"C:\TeraBoxDownload\Projet11.xlsx"

# HELPER FUNCTIONS

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Database connection successful!")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def clean_numeric(value):
    if pd.isna(value) or value == '' or value == 0:
        return None
    try:
        return float(value)
    except:
        return None

def parse_excel_date(date_value):
    """Parse Excel date format"""
    if pd.isna(date_value):
        return None
    try:
        # If it's already a datetime
        if isinstance(date_value, datetime):
            return date_value
        # If it's a number (Excel serial date)
        if isinstance(date_value, (int, float)):
            # Excel epoch starts 1900-01-01
            return pd.Timestamp('1899-12-30') + pd.Timedelta(days=date_value)
        # Try parsing as string
        return pd.to_datetime(date_value)
    except:
        return None

def extract_time_components(date):
    """Extract year, quarter, month from date"""
    if pd.isna(date):
        return None, None, None, None
    year = date.year
    quarter = (date.month - 1) // 3 + 1
    month = date.month
    month_name = date.strftime('%B')
    return year, quarter, month, month_name

# EXTRACT - READ DATA FROM EXCEL

def extract_data():
    """Extract data from Excel file"""
    print("\n" + "="*60)
    print("STEP 1: EXTRACTING DATA FROM EXCEL")
    print("="*60)
    
    try:
        try:
            df = pd.read_excel(EXCEL_FILE, sheet_name='Films')
        except:
            # If 'Films' sheet doesn't exist, try first sheet
            df = pd.read_excel(EXCEL_FILE, sheet_name=0)
        
        print(f"Successfully loaded {len(df)} films from Excel")
        print(f"Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f" Error reading Excel: {e}")
        return None

# TRANSFORM - CLEAN AND PREPARE DATA

def transform_data(df):
    """Transform and clean the data"""
    print("\n" + "="*60)
    print("STEP 2: TRANSFORMING DATA")
    print("="*60)
    
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Parse dates
    print(" Parsing release dates...")
    df_clean['ReleaseDate_Parsed'] = df_clean['ReleaseDate'].apply(parse_excel_date)
    
    # Clean numeric columns
    print("Cleaning numeric values...")
    df_clean['BudgetDollars_Clean'] = df_clean['BudgetDollars'].apply(clean_numeric)
    df_clean['BoxOfficeDollars_Clean'] = df_clean['BoxOfficeDollars'].apply(clean_numeric)
    
    # Calculate derived measures
    print(" Calculating derived measures...")
    df_clean['ProfitDollars'] = df_clean['BoxOfficeDollars_Clean'] - df_clean['BudgetDollars_Clean']
    df_clean['ROI'] = np.where(
        df_clean['BudgetDollars_Clean'] > 0,
        (df_clean['BoxOfficeDollars_Clean'] - df_clean['BudgetDollars_Clean']) / df_clean['BudgetDollars_Clean'],
        None
    )
    
    # Extract time components
    print("Extracting time dimensions...")
    time_components = df_clean['ReleaseDate_Parsed'].apply(
        lambda x: pd.Series(extract_time_components(x))
    )
    df_clean[['Year', 'Quarter', 'Month', 'MonthName']] = time_components
    
    print(f" Data transformation complete!")
    print(f"   - Valid dates: {df_clean['ReleaseDate_Parsed'].notna().sum()}")
    print(f"   - Valid budgets: {df_clean['BudgetDollars_Clean'].notna().sum()}")
    print(f"   - Valid box office: {df_clean['BoxOfficeDollars_Clean'].notna().sum()}")
    
    return df_clean

# LOAD - INSERT DATA INTO DATABASE

def load_dimension_time(df, conn):
    """Load DimTime dimension"""
    print("\n Loading DimTime...")
    
    cursor = conn.cursor()
    
    # Get unique time records
    time_data = df[['RunTimeMinutes', 'ReleaseDate_Parsed', 'Year', 'Quarter', 'Month', 'MonthName']].copy()
    time_data = time_data[time_data['ReleaseDate_Parsed'].notna()].drop_duplicates(subset=['RunTimeMinutes'])
    
    inserted = 0
    errors = 0
    for _, row in time_data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO DimTime (TimeID, FullDate, Year, Quarter, Month, MonthName)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (TimeID) DO NOTHING
            """, (
                int(row['RunTimeMinutes']),
                row['ReleaseDate_Parsed'],
                int(row['Year']) if pd.notna(row['Year']) else None,
                int(row['Quarter']) if pd.notna(row['Quarter']) else None,
                int(row['Month']) if pd.notna(row['Month']) else None,
                row['MonthName']
            ))
            inserted += cursor.rowcount
            conn.commit()
        except Exception as e:
            conn.rollback()
            errors += 1
            if errors <= 3:
                print(f"   Error inserting TimeID {row['RunTimeMinutes']}: {e}")
    
    print(f"   Inserted {inserted} time records")

def load_dimension_film(df, conn):
    """Load DimFilm dimension"""
    print("\n📽️ Loading DimFilm...")
    
    cursor = conn.cursor()
    
    inserted = 0
    errors = 0
    for _, row in df.iterrows():
        try:
            # Handle missing CertificateID values (correct column name!)
            cert_value = None
            if 'CertificateID' in row and pd.notna(row['CertificateID']):
                cert_value = str(int(row['CertificateID']))
            elif 'Certificate' in row and pd.notna(row['Certificate']):
                cert_value = str(int(row['Certificate']))
            
            # Handle missing Review values
            review_value = None
            if pd.notna(row['Review']):
                review_value = str(row['Review'])[:500]
            
            cursor.execute("""
                INSERT INTO DimFilm (FilmID, Title, Certificate, Review)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (FilmID) DO NOTHING
            """, (
                int(row['FilmID']),
                str(row['Title'])[:200],
                cert_value,
                review_value
            ))
            inserted += cursor.rowcount
            conn.commit()
        except Exception as e:
            conn.rollback()
            errors += 1
            if errors <= 5:
                print(f"    Error inserting FilmID {row['FilmID']}: {e}")
    
    print(f"   Inserted {inserted} film records")
    if errors > 0:
        print(f"   {errors} films had errors")

def load_dimension_generic(df, conn, dim_table, id_col, name_col, display_name):
    """Generic function to load simple dimensions"""
    print(f"\n🔹 Loading {display_name}...")
    
    cursor = conn.cursor()
    
    # Get unique values and remove NaN
    unique_values = df[[id_col]].dropna().drop_duplicates()
    
    inserted = 0
    errors = 0
    for _, row in unique_values.iterrows():
        try:
            # Generate a placeholder name
            id_value = int(row[id_col])
            name_value = f"{display_name}_{id_value}"
            
            cursor.execute(f"""
                INSERT INTO {dim_table} ({id_col}, {name_col})
                VALUES (%s, %s)
                ON CONFLICT ({id_col}) DO NOTHING
            """, (id_value, name_value))
            inserted += cursor.rowcount
            conn.commit()
        except Exception as e:
            conn.rollback()
            errors += 1
            if errors <= 3:
                print(f"    Error inserting {id_col} {row[id_col]}: {e}")
    
    print(f"    Inserted {inserted} {display_name} records")

def load_fact_table(df, conn):
    """Load FactFilmPerformance fact table"""
    print("\n Loading FactFilmPerformance...")
    
    cursor = conn.cursor()
    
    inserted = 0
    errors = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO FactFilmPerformance (
                    FilmID, DirectorID, StudioID, GenreID, CountryID, 
                    LanguageID, TimeID, BudgetDollars, BoxOfficeDollars,
                    OscarNominations, OscarWins, RunTimeMinutes,
                    ProfitDollars, ROI
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (FilmID) DO NOTHING
            """, (
                int(row['FilmID']),
                int(row['DirectorID']) if pd.notna(row['DirectorID']) else None,
                int(row['StudioID']) if pd.notna(row['StudioID']) else None,
                int(row['GenreID']) if pd.notna(row['GenreID']) else None,
                int(row['CountryID']) if pd.notna(row['CountryID']) else None,
                int(row['LanguageID']) if pd.notna(row['LanguageID']) else None,
                int(row['RunTimeMinutes']) if pd.notna(row['RunTimeMinutes']) else None,
                row['BudgetDollars_Clean'],
                row['BoxOfficeDollars_Clean'],
                int(row['OscarNominations']) if pd.notna(row['OscarNominations']) else 0,
                int(row['OscarWins']) if pd.notna(row['OscarWins']) else 0,
                int(row['RunTimeMinutes']) if pd.notna(row['RunTimeMinutes']) else None,
                row['ProfitDollars'],
                float(row['ROI']) if pd.notna(row['ROI']) else None
            ))
            inserted += cursor.rowcount
        except Exception as e:
            errors += 1
            conn.rollback()  # ROLLBACK on error
            if errors <= 5:  # Only print first 5 errors
                print(f"    Error inserting FilmID {row['FilmID']}: {e}")
    
    conn.commit()
    print(f"    Inserted {inserted} fact records")
    if errors > 0:
        print(f"   {errors} records failed")

# MAIN ETL PROCESS

def run_etl():
    """Main ETL orchestration"""
    print("\n" + "="*60)
    print(" FILM DATAWAREHOUSE ETL PROCESS")
    print("="*60)
    print(f" Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Extract
    df = extract_data()
    if df is None:
        return
    
    # Step 2: Transform
    df_clean = transform_data(df)
    if df_clean is None:
        return
    
    # Step 3: Load
    conn = get_db_connection()
    if conn is None:
        return
    
    try:
        print("\n" + "="*60)
        print("STEP 3: LOADING DATA INTO DATABASE")
        print("="*60)
        
        # Load dimensions first (in order of dependencies)
        load_dimension_time(df_clean, conn)
        load_dimension_film(df_clean, conn)
        load_dimension_generic(df_clean, conn, 'DimDirector', 'DirectorID', 'DirectorName', 'Director')
        load_dimension_generic(df_clean, conn, 'DimStudio', 'StudioID', 'StudioName', 'Studio')
        load_dimension_generic(df_clean, conn, 'DimGenre', 'GenreID', 'GenreName', 'Genre')
        load_dimension_generic(df_clean, conn, 'DimCountry', 'CountryID', 'CountryName', 'Country')
        load_dimension_generic(df_clean, conn, 'DimLanguage', 'LanguageID', 'LanguageName', 'Language')
        
        # Load fact table last
        load_fact_table(df_clean, conn)
        
        print("\n" + "="*60)
        print(" ETL PROCESS COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        # Verification queries
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM FactFilmPerformance")
        fact_count = cursor.fetchone()[0]
        print(f" Total films in DataWarehouse: {fact_count}")
        
        cursor.execute("""
            SELECT 
                SUM(BudgetDollars) as total_budget,
                SUM(BoxOfficeDollars) as total_boxoffice,
                AVG(ROI) as avg_roi
            FROM FactFilmPerformance
        """)
        stats = cursor.fetchone()
        print(f" Total Budget: ${stats[0]:,.0f}" if stats[0] else "N/A")
        print(f"Total Box Office: ${stats[1]:,.0f}" if stats[1] else "N/A")
        print(f" Average ROI: {stats[2]:.2%}" if stats[2] else "N/A")
        
    except Exception as e:
        print(f"\n ETL failed: {e}")
        conn.rollback()
    finally:
        conn.close()
        print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# RUN THE ETL

if __name__ == "__main__":
    run_etl()