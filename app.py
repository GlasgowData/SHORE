import PHHaB_Partnership as fab 
import asyncio


#pyinstaller  app.py --onefile --console --add-data "PHHaB_Partnership/img/PHHaB-favicon.png;img"


SystemIs = 'SHORE'

if __name__ == "__main__":
    
    
    calendar = fab.PHHaBProcessDataEntrySelection(SystemIs)
    
   
    sps = fab.SPS_Import_Process(calendar)
    asyncio.run(sps.main())
    print('Input Process Complete')
