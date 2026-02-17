import sqlalchemy
import pandas as pd 

index = ['SPIN_Number', 'Sex', 'Birth_Date', 'Forename', 'Surname', 'Record', 'Record_Number', 'Prison_Occasions', 'Unique_Record', 'Live_Record']

Columns = ['Establishment_Name', 'Prisoner_Address_Line_1','Prisoner_Address_Line_2', 'Town', 'Postcode', 'Admission_Date', 'Earliest_Date_of_Liberation', 'Liberation_Reason', 'Local_Authority', 'Date_Received']


i = pd.DataFrame(columns=index)
c = pd.DataFrame(columns=Columns)

df = pd.concat([i, c]).set_index(index)
tablenames = 'SPSData'

engine = sqlalchemy.create_engine(f'sqlite:///{'SPS_Database.sqlite'}')


df.to_sql(tablenames, engine, if_exists='replace')


print('File Creation Complete')