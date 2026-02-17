SHORE (Sustainable Housing On Release for Everyone)

Within Glasgow City Council the SHORE standards have been implemented as the Pathfinder to Health Housing and Benefits PHHaB, however the process invovles receiving 4 excel files. Focusing on utilising the data sharing agreement between the Scottish Prison Service to Local Authorities in Scotland to provide better outcomes for those leaving prison from a short-term sentence

https://www.sps.gov.uk/SHORE
https://glasgowcity.hscp.scot/news/phhab-new-project-launched
https://www.communityjusticeglasgow.org.uk/annualreporter/archive/2020-21/throughcare

This process utilises SQLite database files, due to restrictions on public services this is an easier implementation without the availability of a full SQL Database. A small change to the sqlalchemy codes utilised this is possible to upgrade if that is availible in the Future. The process will combine the 4 Excel documents and create a Database of the data in the SQLite format

Required assets:
1 file SPS_Database.sqlite containing Table: "SPSData"

Run CreateBlankDatabase.py to create the template, this file can be stored elsewhere on the local network of PC, run the set variable to identify the file to the Software

Run app.py where a calendar will appear on the screen. For the first occasion running this the variable and the locations will need to be set where the Database is located as it is a physical file, before proceeding.



