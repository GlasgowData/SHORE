

import shutil
import zipfile
import logging
import pandas as pd
import numpy
import sqlalchemy
import warnings
import asyncio
import datetime as dt
from datetime import date
from pydantic import BaseModel, field_validator, ValidationError
from typing import Optional
from PHHaB_Shared import ProgressBar, LoadMainDatabaseSQL, MainDataBaseSetup, FutureDate, SPS_Data_Setup
# from pandas.api.types import CategoricalDtype



class data_verifiction:
    """Verification that data received by SPS is the correct file types"""
    
    def __init__(self, df1Input, df2Input, df3Input, df4Input):
        self.df1Input = df1Input
        self.df2Input = df2Input
        self.df3Input = df3Input
        self.df4Input = df4Input
        self.df1Output = pd.DataFrame()
        self.df2Output = pd.DataFrame()
        self.df3Output = pd.DataFrame()
        self.df4Output = pd.DataFrame()
        

    class SPS_Report_01And03(BaseModel):
        Prisoner_Number: int
        Surname: str
        Forename: str
        Prisoner_Gender: str
        DOB: dt.date
        Address: str
        Prisoner_Address_Line_2: str
        Town: str
        Postcode: str
        Establishment_Name: str
        Admission_Date: Optional[dt.date]
        EDL: Optional[dt.date]  # Allow blank
        Local_Authority: str

        @field_validator('DOB', 'Admission_Date', 'EDL', mode='before')
        def parse_custom_date(cls, value, info):
            if isinstance(value, dt.date):
                return value
                            
            if value in (None, ''):
                if info.field_name == 'EDL':
                    return None
                raise ValueError(f"{info.field_name} cannot be blank.")


            formats = [
                "%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d",
                "%d-%m-%Y", "%d %b %Y", "%b %d, %Y"
            ]

            for fmt in formats:
                try:
                    return dt.strptime(value, fmt).date()
                except ValueError:
                    continue

            raise ValueError(
                f"{info.field_name} format not recognized. Please use one of: "
                "DD/MM/YY, YYYY-MM-DD, DD-MM-YYYY, '06 Aug 2025', or 'Aug 6, 2025'"
            )

        @classmethod
        def from_row(cls, row):
            return cls(**row)
    

    class SPS_Report_02(BaseModel):
        Prisoner_Number: int
        Surname: str
        Forename: str
        Prisoner_Gender: str
        DOB: dt.date
        Last_Establishment : str
        Address: str
        Prisoner_Address_Line_2: str
        Liberation_Reason : Optional[str]
        Town: str
        Postcode: str
        Local_Authority: str

        @field_validator('DOB', mode='before')
        def parse_custom_date(cls, value, info):
            if isinstance(value, dt.date):
                return value
                   
            formats = [
                "%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d",
                "%d-%m-%Y", "%d %b %Y", "%b %d, %Y"
            ]

            for fmt in formats:
                try:
                    return dt.strptime(value, fmt).date()
                except ValueError:
                    continue

            raise ValueError(
                f"{info.field_name} format not recognized. Please use one of: "
                "DD/MM/YY, YYYY-MM-DD, DD-MM-YYYY, '06 Aug 2025', or 'Aug 6, 2025'"
            )

        @classmethod
        def from_row(cls, row):
            return cls(**row)

    class SPS_Report_04(BaseModel):
        Prisoner_Number: int
        Surname: str
        Forename: str
        DOB: dt.date
        Last_Establishment : str        
        Address: str
        Prisoner_Address_Line_2: str
        Town: str
        Postcode: str
        EDL: Optional[dt.date]  # Allow blank
        Local_Authority: str

        @field_validator('DOB','EDL', mode='before')
        def parse_custom_date(cls, value, info):
            if isinstance(value, dt.date):
                return value
                            
            if value in (None, ''):
                if info.field_name == 'EDL':
                    return None
                raise ValueError(f"{info.field_name} cannot be blank.")


            formats = [
                "%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d",
                "%d-%m-%Y", "%d %b %Y", "%b %d, %Y"
            ]

            for fmt in formats:
                try:
                    return dt.strptime(value, fmt).date()
                except ValueError:
                    continue

            raise ValueError(
                f"{info.field_name} format not recognized. Please use one of: "
                "DD/MM/YY, YYYY-MM-DD, DD-MM-YYYY, '06 Aug 2025', or 'Aug 6, 2025'"
            )

        @classmethod
        def from_row(cls, row):
            return cls(**row)


    def catch_errors(self, dataFrame, method):
        valid_records = []
        from_row = method.from_row
        rows = dataFrame.to_dict(orient="records")
        for index, row in enumerate(rows):
            try:
                record = from_row(row)
                valid_records.append(record.model_dump())  
                pass
            # except ValidationError as e:
            #     
            except ValidationError as exc:
                for err in exc.errors():
                    error_type = err['type']
                    msg = err['msg']
                    loc = err['loc']
                

                    try:
                        spin = err.get('input', {}).get('Prisoner_Number', None)
                        print(f"\n❌ Invalid Records {spin}: {loc} {error_type} {msg}")
                    except:
                            print(f"\n❌ Invalid Record index: {index}: {loc} {error_type} {msg}")    
                            
        return pd.DataFrame(valid_records) 
                            
    async def main(self):
        try:
            async with asyncio.TaskGroup() as group:
                self.df1Output = self.catch_errors(self.df1Input, self.SPS_Report_01And03)
                self.df2Output = self.catch_errors(self.df2Input, self.SPS_Report_02)
                self.df3Output = self.catch_errors(self.df3Input, self.SPS_Report_01And03)
                self.df4Output = self.catch_errors(self.df4Input, self.SPS_Report_04)
        except ValidationError as exc:
            for err in exc.errors():
                raise SystemExit(f'Validation of SPS Files has failed {err}')
        
              


class LoadSpsZipfile:
        def __init__(self, zip_file, import_date, reportfiles):   
                self.zip_path = zip_file
                self.import_date = import_date
                self.reportfiles = reportfiles
                self.report1 = pd.DataFrame()
                self.report2 = pd.DataFrame()
                self.report3 = pd.DataFrame()
                self.report4 = pd.DataFrame()
                self.FinalWeeklyReport = pd.DataFrame()
                self.report_name = ('1. Admission', 
                                '2. Scheduled - Liberation', 
                                '4. Liberated',
                                '5. Convicted & Liberated')
                
        def ColumnsToAdd(self):
            """Columns need to added to weekly files to allow for 
            merge with SQL File"""
            
            self.FinalWeeklyReport = self.FinalWeeklyReport.assign(Prison_Occasions = "")
            self.FinalWeeklyReport = self.FinalWeeklyReport.assign(Live_Record = "")
            self.FinalWeeklyReport = self.FinalWeeklyReport.assign(Unique_Record = "")
            self.FinalWeeklyReport = self.FinalWeeklyReport.assign(Record_Number = "")



        def check_column_titles(self):

                """To ensure all columns are correctly picked by the user and SPS has not inserted a new column
                or similarly named column, ths function will check the column names of each report received
                from the Scottish Prison Service.

                Using the df.column pandas function an array is created of the column names whcih is then checked
                against an array of what the column names should be.

                Another variable is input as a count of how many times the function has been run which tells the function
                what report should be being input and what column headers are being used.

                If column names do not match then function exits the programme with a warning message for the
                user to check column names or possible wrong input"""

                # Array is what should be checked against to ensure correct input and renaming of all columns
                reports = [
                            ['Report File 1',
                            ['Prisoner Number', 'Surname', 'Forename', 'Prisoner Gender', 'DOB',
                            'Address ', 'Prisoner Address Line 2', 'Town', 'Postcode',
                            'Establishment Name', 'Admission Date', 'EDL', 'Local Authority']],
                            
                            ['Report File 2',
                            ['Prisoner Number', 'Surname', 'Forename', 'Prisoner Gender', 'DOB',
                            'Last Establishment', 'Address ', 'Prisoner Address Line 2',
                            'Liberation Reason', 'Town', 'Postcode', 'Local Authority']],
                            
                            ['Report File 3',
                            ['Prisoner Number', 'Surname', 'Forename', 'Prisoner Gender', 'DOB',
                            'Address ', 'Prisoner Address Line 2', 'Town', 'Postcode',
                            'Establishment Name', 'Admission Date', 'EDL', 'Local Authority']],
                            
                            ['Report File 4',
                            ['Prisoner Number', 'Surname', 'Forename', 'DOB', 'Last Establishment',
                            'Address ', 'Prisoner Address Line 2', 'Town', 'Postcode', 'Edl',
                            'Local Authority']]]

                reportcolls = [self.report1.columns, self.report2.columns, self.report3.columns, self.report4.columns]
                
                for (check, actual) in zip(reports, reportcolls):
                    if numpy.array_equal(check[1], actual):
                        pass
                    else:
                        raise ValueError(f"Column mismatch in report {check[0]}\
                                         \n\n\n Columns in Report {actual}\n\n\nColumns should be{check[1]}")
                

        
        def read_files(self):
                #Create zip file archive
                archive = zipfile.ZipFile(self.zip_path, 'r')
                
                file_list_from_zip = {f.filename.split('/')[1] for f in archive.filelist}
                all_included = all(item in file_list_from_zip for item in self.reportfiles)
                #Are 4 files included 
                if all_included != True:
                        logging.error('🛠️ Error, file count is not 4 within zip file please check file')
                        quit()
                
                #File check
                #ensure the files are correctly uploaded and the column titles are correct
                # then assigne to report1-4 variables
                with warnings.catch_warnings(record=True):
                        warnings.simplefilter('always')
                        
                        # ziparchivefile = archive.open(reportfiles[0])
                        
                        self.report1 = pd.read_excel(archive.open(f"{self.reportfiles[0]}"), engine="openpyxl")

                        self.report2 = pd.read_excel(archive.open(f"{self.reportfiles[1]}"), engine="openpyxl")

                        self.report3 = pd.read_excel(archive.open(f"{self.reportfiles[2]}"), engine="openpyxl")
                   
                        self.report4 = pd.read_excel(archive.open(f"{self.reportfiles[3]}"), engine="openpyxl")
                        

        def add_record_num(self):
                #Add Record Name
                self.report1 = self.report1.assign(Record = self.report_name[0])
                self.report2 = self.report2.assign(Record = self.report_name[2])
                self.report3 = self.report3.assign(Record = self.report_name[1])
                self.report4 = self.report4.assign(Record = self.report_name[3])         
                


        def rename_columns(self):
                #Rename Columns
                self.report1 = self.report1.rename(columns={'Prisoner Number': 'SPIN_Number',
                                                                'Prisoner Gender': 'Sex',
                                                                'DOB': 'Birth_Date',
                                                                'Address ': 'Prisoner_Address_Line_1',
                                                                'Prisoner Address Line 2': 'Prisoner_Address_Line_2',
                                                                'Establishment Name': 'Establishment_Name',
                                                                'Admission Date': 'Admission_Date',
                                                                'EDL': 'Earliest_Date_of_Liberation',
                                                                'Local Authority': 'Local_Authority'
                                                            })

                self.report2 = self.report2.rename(columns={'Prisoner Number': 'SPIN_Number',
                                                                'Prisoner Gender': 'Sex',
                                                                'DOB': 'Birth_Date',
                                                                'Last Establishment': 'Establishment_Name',
                                                                'Address ': 'Prisoner_Address_Line_1',
                                                                'Prisoner Address Line 2': 'Prisoner_Address_Line_2',
                                                                'Liberation Reason': 'Liberation_Reason',
                                                                'Local Authority': 'Local_Authority'
                                                            })

                self.report3 = self.report3.rename(columns={'Prisoner Number': 'SPIN_Number',
                                                                'Prisoner Gender': 'Sex',
                                                                'DOB': 'Birth_Date',
                                                                'Address ': 'Prisoner_Address_Line_1',
                                                                'Prisoner Address Line 2': 'Prisoner_Address_Line_2',
                                                                'Establishment Name': 'Establishment_Name',
                                                                'Admission Date': 'Admission_Date',
                                                                'EDL': 'Earliest_Date_of_Liberation',
                                                                'Local Authority': 'Local_Authority'
                                                            })
                
                self.report4 = self.report4.rename(columns={'Prisoner Number': 'SPIN_Number',
                                                                'DOB': 'Birth_Date',
                                                                'Last Establishment': 'Establishment_Name',
                                                                'Address ': 'Prisoner_Address_Line_1',
                                                                'Prisoner Address Line 2': 'Prisoner_Address_Line_2',
                                                                'Edl': 'Earliest_Date_of_Liberation',
                                                                'Local Authority': 'Local_Authority'
                                                    })
                
        def combine_weekly_files(self):
            self.FinalWeeklyReport = pd.concat([self.report1, self.report2, self.report3, self.report4])
            
            
        def Input_Gender(self):
            # Replace M and F with Male and Female
            self.FinalWeeklyReport.Sex = self.FinalWeeklyReport[['Sex']].replace({'M': 'Male', 'F': 'Female'})
            

        def Expand_Prison_Name(self):
                # Replace Abreviated Version of the Prison Establishment name
                self.FinalWeeklyReport.Establishment_Name = \
                self.FinalWeeklyReport.Establishment_Name.replace({'AW': 'Addiewell',
                                                                        'BA': 'Barlinnie',
                                                                        'CV': 'Cornton Vale',
                                                                        'DF': 'Dumfries',
                                                                        'ED': 'Edinburgh',
                                                                        'GN': 'Grampian',
                                                                        'GO': 'Glenochil',
                                                                        'GR': 'Greenock',
                                                                        'IN': 'Inverness',
                                                                        'KM': 'Kilmarnock',
                                                                        'LM': 'Low Moss',
                                                                        'OP': 'Castle Huntly',
                                                                        'PL': 'Polmont',
                                                                        'PR': 'Perth',
                                                                        'SO': 'Shotts',
                                                                        'LL' : 'Lilias Centre',
                                                                        'ST' : 'Stirling'})
                

                
        def add_import_date(self):
                self.FinalWeeklyReport = self.FinalWeeklyReport.assign(Date_Received = self.import_date)


        def set_WeeklyReport_data_types(self):
            new_columns = ['Record_Number', 'Prison_Occasions', 'Unique_Record', 'Live_Record']
            
            self.FinalWeeklyReport = self.FinalWeeklyReport.assign(**{col: '' for col in new_columns})

            sps = MainDataBaseSetup(self.FinalWeeklyReport) 
            self.FinalWeeklyReport = sps.df.reset_index()
            
            
        def sort_before_merger(self):
                self.FinalWeeklyReport = self.FinalWeeklyReport.sort_values('SPIN_Number', ascending=True)

        def rename_uploaded_sps_files(self, savetopath):
                date = self.import_date.strftime('%d-%m-%Y')

                with zipfile.ZipFile(self.zip_path, 'r') as zip: 
                        
                        reportfiles = zip.filelist

                        for savefile in reportfiles: 
                                zip.extract(savefile, path=savetopath)
                                #Rename File
                                
                                shutil.move(savetopath +'/'+ savefile.filename, savetopath +'/'+savefile.filename.split(".")[0] + date + "." + savefile.filename.split(".")[1])
            
            


class CompleteDataBase:
    """Data has been sorted and merged, 
    class is to add specifics of the database not supplied by SPS"""
    
    def __init__(self, MainDatabase, DatabasefileLocation):
        self.MainDatabase = MainDatabase
        self.DatabasefileLocation = DatabasefileLocation
    
      
    def Sex_columns(self):
    
        # Load known corrections
        # loadlistcheck = pd.read_pickle('C:/Users/hillr/PycharmProjects/PHHaBV10_Test/Database/SexCheck.pkl')
        # loadlistcheck['SPIN_Number'] = loadlistcheck['SPIN_Number'].astype('int64[pyarrow]')
        
        # # Apply known corrections
        # self.MainDatabase.loc[
        #     self.MainDatabase.SPIN_Number.isin(loadlistcheck['SPIN_Number']),
        #     'Sex'
        # ] = 'Female'

        # Get records with known Sex values
        known_sex = self.MainDatabase[['SPIN_Number', 'Sex']].dropna().drop_duplicates()

        # Get records with missing Sex
        missing_sex = self.MainDatabase[self.MainDatabase['Sex'].isnull()][['SPIN_Number']]


        # Merge to fill missing Sex values
        filled_sex = missing_sex.merge(known_sex, on='SPIN_Number', how='left')

        # Update MainDatabase with filled values
        self.MainDatabase = self.MainDatabase.merge(
            filled_sex[['SPIN_Number', 'Sex']],
            on='SPIN_Number',
            how='left',
            suffixes=('', '_filled')
        )

        # Fill missing Sex values from merged column
        self.MainDatabase['Sex'] = self.MainDatabase['Sex'].combine_first(self.MainDatabase['Sex_filled'])

        # Drop helper column
        self.MainDatabase.drop(columns=['Sex_filled'], inplace=True)


    def add_occasions_total(self):
        
        """Count the occasions a prisoner has been in an SPS establishment."""

        # Ensure the DataFrame is sorted by SPIN_Number and row order
        self.MainDatabase = self.MainDatabase.sort_values(by=['SPIN_Number', 'Date_Received'])
        
        # Identify new admissions
        is_new_admission = (self.MainDatabase['Record'] == '1. Admission')

        # Mark where a new occasion starts: True if it's a new admission or a new SPIN_Number
        new_occasion = is_new_admission & (self.MainDatabase['SPIN_Number'] == self.MainDatabase['SPIN_Number'].shift())

        # Start a new count when SPIN_Number changes or a new admission occurs
        group_change = (self.MainDatabase['SPIN_Number'] != self.MainDatabase['SPIN_Number'].shift()) | new_occasion
        group_change = group_change.fillna(True).astype(int)


        # Cumulative sum of group changes per SPIN_Number
        # self.MainDatabase['Prison_Occasions'] = group_change.groupby(self.MainDatabase['SPIN_Number']).cumsum().astype(int)

        self.MainDatabase['Prison_Occasions'] = (
            group_change.groupby(self.MainDatabase['SPIN_Number'])
            .cumsum()
            .fillna(0)
            .astype(int)
        )




    def add_record_number(self):
            
            """Creates a 3 digit Record Number"""
            def three_digit_string(countnum):
                return str(countnum).zfill(3)
            
            #Create Record_Number columns
            self.MainDatabase.assign(Record_Number = "None")
            
            
            # Ensure SPIN_Number is integer type and drop decimal part
            self.MainDatabase['SPIN_Number'] = pd.to_numeric(self.MainDatabase['SPIN_Number'], errors='coerce').fillna(0).astype(int)
                            
            self.MainDatabase['Record_Number'] = (
                self.MainDatabase['SPIN_Number'].astype(str) + "-" +
                self.MainDatabase['Prison_Occasions'].apply(lambda x: f"{int(x):03}")
)



    def add_unique_id(self):
        #find final record in each record number
        self.MainDatabase['Unique_Record'] = ~(
            self.MainDatabase['Record_Number'] == self.MainDatabase['Record_Number'].shift(-1))
        #Changes bool value to 1 or zero
        self.MainDatabase['Unique_Record'] = self.MainDatabase['Unique_Record'].astype(int)
    
    
    def add_missing_sps_data(self):
        entrygroup = self.MainDatabase[['Record_Number', 'Admission_Date']].dropna().drop_duplicates().sort_values(['Record_Number', 'Admission_Date'])
        exitgroup = self.MainDatabase[['Record_Number', 'Earliest_Date_of_Liberation']].dropna().drop_duplicates().sort_values(['Record_Number', 'Earliest_Date_of_Liberation'])

        entrygroup = entrygroup.drop_duplicates(subset='Record_Number', keep='last').set_index('Record_Number')
        exitgroup = exitgroup.drop_duplicates(subset='Record_Number', keep='first').set_index('Record_Number')

        # Admission Dates
        mask_adm = (self.MainDatabase['Record'] != '1. Admission') & (self.MainDatabase['Admission_Date'].isnull())
        self.MainDatabase.loc[mask_adm, 'Admission_Date'] = self.MainDatabase.loc[mask_adm, 'Record_Number'].map(entrygroup['Admission_Date'])

        # Liberation Dates
        mask_lib = (self.MainDatabase['Record'] != '1. Admission') & (self.MainDatabase['Earliest_Date_of_Liberation'].isnull())
        
        self.MainDatabase.loc[mask_lib, 'Earliest_Date_of_Liberation'] = self.MainDatabase.loc[mask_lib, 'Record_Number'].map(exitgroup['Earliest_Date_of_Liberation'])

    def copy_liberations_reasons_2Record4(self):
        reasons = self.MainDatabase[self.MainDatabase['Record'] == '4. Liberated'][['Record_Number', 'Liberation_Reason']]
        reasons = reasons.drop_duplicates(subset='Record_Number', keep='last').set_index('Record_Number')

        mask = (self.MainDatabase['Record'] == '5. Convicted & Liberated') & (self.MainDatabase['Liberation_Reason'].isnull())
        
        self.MainDatabase.loc[mask, 'Liberation_Reason'] = self.MainDatabase.loc[mask, 'Record_Number'].map(reasons['Liberation_Reason'])


    def latest_date_imported(self):
        self.latest_record = self.MainDatabase['Date_Received'].dropna().max()
        
    
    def Untried_warrants_record(self, chunk_size=100_000):
        df = self.MainDatabase
        num_chunks = len(df) // chunk_size + 1
        updated_chunks = []

        # Precompute already assigned warrants
        already_assigned = set(
            df.loc[df['Record'] == '3. Untried - Existing Warrant', 'Record_Number']
        )

        for i in range(num_chunks):
            chunk = df.iloc[i * chunk_size : (i + 1) * chunk_size]

            # Filter live records with valid liberation dates
            utw = chunk.query(
                "Record in ['1. Admission', '2. Scheduled - Liberation'] and "
                "Live_Record == 1 and Earliest_Date_of_Liberation.notnull()",
                engine='python'
            )

            # Identify dates that should have triggered liberation
            should_have_gone_dates = set(
                utw.loc[utw['Earliest_Date_of_Liberation'] < self.latest_record, 'Earliest_Date_of_Liberation']
            )

            # Filter untried records based on those dates
            untired = chunk.query(
                "Record in ['1. Admission', '2. Scheduled - Liberation'] and "
                "Live_Record == 1 and Earliest_Date_of_Liberation in @should_have_gone_dates",
                engine='python'
            )

            # Exclude already assigned
            untired = untired[~untired['Record_Number'].isin(already_assigned)]

            # Update fields
            chunk.loc[untired.index, 'Record'] = '3. Untried - Existing Warrant'
            chunk.loc[untired.index, 'Live_Record'] = 0

            updated_chunks.append(chunk)

        # Recombine and sort
        self.MainDatabase = pd.concat(updated_chunks, ignore_index=True)
        self.MainDatabase.sort_values(
            by=['SPIN_Number', 'Date_Received', 'Record'],
            ascending=True,
            inplace=True
        )
    
    
    def assign_live(self):
        # Default all to 0
        self.MainDatabase.assign(Live_Record =  0)

        # Find the last record for each SPIN_Number
        last_indices = self.MainDatabase.groupby('SPIN_Number').tail(1).index

        # Assign 1 to those records
        self.MainDatabase.loc[last_indices, 'Live_Record'] = 1
        
        
        self.MainDatabase['Live_Record'] = (
                                            pd.to_numeric(self.MainDatabase['Live_Record'], errors='coerce')
                                            .fillna(0)
                                            .astype('Int64')  # Capital 'I' for nullable integer
                                            )


    

    def ReOrder_set_multiindex(self):
        """Set order of columns 
        and Sets multi-index"""
        
        sps = MainDataBaseSetup(self.MainDatabase) 
        self.MainDatabase = sps.df
        
        
        


                    
    def Expand_liberation_reasons(self):
        """Expands the short hand liberation 
        reasons provided by SPS """
        
        self.MainDatabase.Liberation_Reason = self.MainDatabase.Liberation_Reason.replace(
                                {'Lib Sent. Exp.' : 'Liberation_Sentence_Expired',
                                'Lib From Court' : 'Liberation_from_Court', 
                                'Lib To Mental Hosp' : 'Liberation_to_Mental_Health_Establishment',
                                'Lib On Parole' : 'Liberation_on Parole',
                                'Lib To S.R.O' : 'Liberation_to_Supervised_Release_Orders', 
                                'To Bail' : 'Bailed', 
                                'Lib On Licence' : 'Liberation_on_Licence',
                                'Deceased' : 'Deceased', 
                                'Discretionary Early Release' : 'Discretionary_Early_Release',
                                'Coronavirus (Scotland) Act 2020 Regs' : 'Coronavirus_(Scotland)_Act_2020_Regs', 
                                'Proc. Fiscal' : 'Crown_Office_and_Procurator_Fiscal_Service',
                                'Lib Fine Paid' : 'Liberated_paid_fine', 
                                'Lib On Appeal' : 'Liberated_on_appeal', 
                                'Lib To Interlib' : 'Liberation_to_interim_liberation',
                                'Duplicate Record' : 'Duplicate_Record', 
                                'Lib Imm. Author' : 'Liberation to Immigration Authority', 
                                'Lib Dungavel DC' : 'Dungavel_Immigration_Removal_Centre',
                                'Deported' : 'Deported',
                                'Lib To E.R.S' : 'Early_Release_Scheme', 
                                'Lib To List D' : 'Liberated_to_D_List_School/Secure_Unit'})
            

    
    def commit_to_sql(self):
        """Commit SPS Database to the SQLiteFile\n
        !!!!Ensure the multi index is in place prior to sending to SQL!!!\n
        Multi Index columns are 
        """
        setup = SPS_Data_Setup()
        
        engine = sqlalchemy.create_engine(f'sqlite:///{self.DatabasefileLocation}')
                    
        self.MainDatabase.to_sql('SPSData', engine, 
                                    index_label=setup.index_columns, 
                                        if_exists='replace')
        engine.dispose()




class SPS_Import_Process:
    def __init__(self, CalendarClass):
        
        """Importing the SPS 4 Data files on a weekly basis from a ZIP file.\n
        Class requires\n
        Location of SQLite database on shared drive or hard drive\n
        Calendar_selection_class: which gives the files location and date of import\n
        Reports_folder: where the SPS 4 reports will be renamed with import date to be stored\n
        \n
        \n
        Output will be 4 files in the reports folder and commitment to Sqlite file of new files"""
        self.start_date = self.__get_start_date(CalendarClass.config_variables['startDate']) # Significantly this date must be completed as the first week a report was received on a monday 
        self.spsDataBaseLocation = CalendarClass.config_variables['spsDataBaseLocation']
        self.reportslocation = CalendarClass.config_variables['reportslocation']
        self.files_tuple = CalendarClass.file_selected
        self.import_date = CalendarClass.import_date
        self.MainDatabase = self.read_sql_file()
        self.AllDates = self.check_all_files_received()
        self.progress = ProgressBar()
        
        self.reportfiles = ['LA Report 1 Scheduled.xlsx',
                'LA Report 2 Liberations Scheduled.xlsx',
                'LA Report 3 Scheduled.xlsx',
                'LA Report 4 Scheduled.xlsx']
            
    def __get_start_date(self, date_str):

        datelist = date_str.split("/")

        newdateformat = date(
            day=int(datelist[1]),
            month=int(datelist[0]),
            year=int(datelist[2])
        )

        return newdateformat
        
        
    def check_all_files_received(self):
        """
        Checks for missing Monday report dates from SPS starting from self.start_date to today.
        If any dates are missing, logs them. Otherwise, confirms all dates are present.
        """

        #Import date cannot be in the future
        if self.import_date > dt.date.today():
            
            f = FutureDate()

            raise SystemExit(f"\n{f.retrieve()}\n❌❌❌ Import date is in the future 📆")
            
        
        #Import Date cannot be input again
        
        # Get unique received dates from the database
        received_dates = pd.Series(self.MainDatabase['Date_Received'].dropna().unique())
        
        received_dates = [d if isinstance(d, dt.date) else d.date() for d in received_dates]
        
        #Does the import date already exist if so exit 
        if self.import_date in received_dates:
            raise SystemExit(f"\n❌ Date submitted duplicated import date {self.import_date.strftime('%d-%m-%Y')}")
        
        
        #import date cannot be anything other than a Monday
        #Run all dates on Mondays from start date
        #to todays date, are all mondays
        #Add imported Date
        received_dates = list(received_dates) + [self.import_date]
        
        # Get all Mondays from start_date to today
        expected_dates = pd.date_range(start=self.start_date, end=dt.date.today(), freq='W-MON')

        # Convert to list of date objects
        expected_dates = [d.date() for d in expected_dates]

        #If date of report is not a Monday Exit
        if self.import_date not in expected_dates:
            raise SystemExit(f"\n❌❌❌ Error \nDate submitted: {self.import_date.strftime('%d-%m-%Y')} is not a Monday \nSPS report dates can only be a Monday 📆")
    

        # Identify missing expected Mondays
        missing_dates = [d for d in expected_dates if d not in received_dates]
        self.missing_dates = missing_dates

        if not missing_dates:
            return True
        else:
            missing_df = pd.DataFrame(missing_dates, columns=["Missing Dates"])
            logging.info("\n❌ Missing Dates:\n%s", missing_df)
            return False

        
    class MissingImportDatesError(Exception):
        def __init__(self, missing_dates):
            self.missing_dates = missing_dates or []
            dates_str = ", ".join(d.strftime("%d-%m-%Y") for d in self.missing_dates)
            message = f"Not all import dates included. Missing: {dates_str}"
            super().__init__(message)

            
    def read_sql_file(self):
        sql = LoadMainDatabaseSQL(self.spsDataBaseLocation)
        sql.read_existing_database_file()
        # sql.exclude_date_for_testing(2025,10,6)
        return sql.sql_file

    async def main(self):
        
        self.progress.Process('SPS Input')
        self.progress.update(5)
        
        # self.read_sql_file()
        
        zipf = LoadSpsZipfile(self.files_tuple, self.import_date, self.reportfiles)
        
        self.progress.update(15)
        
        async with asyncio.TaskGroup() as group:
                zipf.read_files()
                zipf.rename_uploaded_sps_files(self.reportslocation)

        
        self.progress.update(20)
        
        zipf.check_column_titles()
        zipf.add_record_num()
        zipf.rename_columns()
        zipf.combine_weekly_files()
        
        self.progress.update(30)
        
        async with asyncio.TaskGroup() as group:
            zipf.Input_Gender()
            zipf.Expand_Prison_Name()
            zipf.add_import_date()
        
        self.progress.update(35)
        
        zipf.set_WeeklyReport_data_types()

        self.progress.update(40)
        
        zipf.ColumnsToAdd() 
        
        self.ExportDataBase = pd.concat([self.MainDatabase.reset_index(drop=False), 
                                        zipf.FinalWeeklyReport.reset_index(drop=False)])
        
        
        self.progress.update(45)
        
        self.ExportDataBase = self.ExportDataBase.sort_values(by=['SPIN_Number', 
                                                                'Date_Received', 
                                                                'Record'], 
                                                                ascending=True)
        
        
        self.progress.update(50)
        
        # self.ExportDataBase = self.ExportDataBase.reset_index(drop=True)
        
        #Complete all the SPS Database
        fix = CompleteDataBase(self.ExportDataBase, self.spsDataBaseLocation)
        
        fix.Sex_columns()
        
        self.progress.update(55)
        
        async with asyncio.TaskGroup() as group:
            fix.add_occasions_total()
            fix.add_record_number()
            fix.add_unique_id()
        
        self.progress.update(60)
        
        async with asyncio.TaskGroup() as group:
            fix.add_missing_sps_data()
            fix.copy_liberations_reasons_2Record4()
            fix.latest_date_imported()
            
        self.progress.update(70)
        
        #Add untried Warrants Outstanding Record Type
        fix.Untried_warrants_record()

        self.progress.update(80)

        async with asyncio.TaskGroup() as group:
            fix.assign_live()
            fix.Expand_liberation_reasons()
            
        self.progress.update(95)    
        
        fix.ReOrder_set_multiindex()
            
        fix.commit_to_sql()
        

        del self.MainDatabase #Clear old data from the Sql Load
        #Pass MainDatabase to reporting 
        self.MainDatabase = fix.MainDatabase.copy()
        
        self.progress.update(100)
