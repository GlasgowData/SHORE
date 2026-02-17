import pandas as pd 
import sqlalchemy 
from pandas.api.types import CategoricalDtype
import logging 
from dataclasses import dataclass
import datetime as dt

@dataclass
class SPS_Data_Setup:
    index_columns = ['SPIN_Number', 'Sex', 'Birth_Date',  
                        'Forename', 'Surname', 'Record', 'Record_Number',
                        'Prison_Occasions', 'Unique_Record', 'Live_Record']
    
    ColumnOrder = ['Establishment_Name', 'Prisoner_Address_Line_1', 'Prisoner_Address_Line_2', 
                    'Town', 'Postcode', 'Admission_Date', 'Earliest_Date_of_Liberation', 'Liberation_Reason', 
                    'Local_Authority', 'Date_Received',]
    
    Types =  {'SPIN_Number': 'int64[pyarrow]',
                 'Record_Number': 'string[pyarrow]',
                'Unique_Record': 'int64[pyarrow]',
                'Prison_Occasions': 'int64[pyarrow]',
                'Live_Record': 'int64[pyarrow]',
                'Birth_Date': 'datetime64[ns]',
                'Forename': 'string[pyarrow]',
                'Surname': 'string[pyarrow]',
                'Sex': 'string[pyarrow]',
                'Establishment_Name': 'string[pyarrow]',
                'Prisoner_Address_Line_1': 'string[pyarrow]',
                'Prisoner_Address_Line_2': 'string[pyarrow]',
                'Town': 'string[pyarrow]',
                'Postcode': 'string[pyarrow]',
                'Admission_Date': 'datetime64[ns]',
                'Earliest_Date_of_Liberation': 'datetime64[ns]',
                'Liberation_Reason': 'string[pyarrow]',
                'Local_Authority': 'string[pyarrow]',
                'Date_Received': 'datetime64[ns]',
                
                'Record': CategoricalDtype(
                    categories=[
                        "1. Admission",
                        "2. Scheduled - Liberation",
                        "3. Untried - Existing Warrant",
                        "4. Liberated",
                        "5. Convicted & Liberated"
                    ],
                    ordered=True
                ),
                 }


class MainDataBaseSetup:
    """
    Pass the DataFrame (Weekly Combined or SQL load)
    to automatically apply data types and structure.
    """

    def __init__(self, df):
        self.setup = SPS_Data_Setup()
        self.df = df.copy()  # Avoid modifying original
        self.prepare_dataframe()

    def prepare_dataframe(self):
        try:
            # Step 1: Reset index if it's a MultiIndex
            if isinstance(self.df.index, pd.MultiIndex):
                if list(self.df.index.names) == self.setup.index_columns:
                    self.df = self.df.reset_index()
                else:
                    logging.warning("MultiIndex found but does not match expected structure.")

            # Step 2: Apply types
            for col, dtype in self.setup.Types.items():
                if col not in self.df.columns:
                    logging.warning(f"Column '{col}' not found in DataFrame.")
                    continue

                if 'datetime' in str(dtype):
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce').dt.date
                else:
                    self.df[col] = self.df[col].astype(dtype)

            # Step 3: Reorder and set MultiIndex
            try:
                self.df = self.df[self.setup.ColumnOrder + self.setup.index_columns]
                self.df = self.df.set_index(self.setup.index_columns)
            except Exception as e:
                logging.error(f"Failed to create MultiIndex: {e}")
                raise
        except Exception as e:
            logging.error(f"Failed to prepare DataFrame: {e}")
            raise


class LoadMainDatabaseSQL:
        
        """
            DatabasefileLocation variable needs to be set 
        
            Run read_existing_database_file
            to read data
            
        """
        
        def __init__(self, DatabasefileLocation):     
                self.DatabasefileLocation = DatabasefileLocation
                self.sql_file = pd.DataFrame()
                self.sps = SPS_Data_Setup()
                
        
        def read_existing_database_file(self):
            
            try:
                engine = sqlalchemy.create_engine(f'sqlite:///{self.DatabasefileLocation}')
                chunks = pd.read_sql('SPSData', engine, chunksize=50000)
                frames = list(chunks)
                self.sql_file = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                engine.dispose() 
                  
                #set types and order of columns 
                #Then set index              
                sps = MainDataBaseSetup(self.sql_file) 
                self.sql_file = sps.df
                   

            except Exception as e:
                logging.error(f"Failed to read database: {e}")
                raise


