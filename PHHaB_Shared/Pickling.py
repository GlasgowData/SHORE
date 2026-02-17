import pickle
import os


class FutureDate:
    """Send data to Computers RAM for storage and retrieval later"""
    
    def __init__(self):
        self.store = pickle.dumps("""\n⣿⣿⣿⣿⣿⣿⣿⠋⣤⣭⡻⡿⣿⣿⡿⠿⠿⠿⢿⣿⡿⣿⢋⣥⡌⢻⣿⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⣿⣷⣝⡿⠋⠀⠈⢀⡴⠒⠒⠲⣄⠈⠀⠀⠙⠟⣡⣿⣿⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⢸⡁⢾⣿⡷⢸⠀⠀⠀⠀⢠⣿⣿⣿⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⣿⣿⡟⠀⠀⠀⠀⠈⠳⠤⡡⠴⠋⠀⠀⠀⠀⠈⣿⣿⣿⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⣿⣿⡷⠶⠶⠶⢶⡶⠶⠶⠶⠶⠶⣶⠶⠶⠶⠶⣾⣿⣿⣿⣿⣿\n⣿⠿⠛⠛⢿⣿⣿⣿⠁⠀⠀⠀⢸⡇⠀⠀⠀⠀⠀⣿⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿\n⠃⠀⠀⠀⠀⢹⣿⣿⣀⣀⣀⣀⣸⣇⣀⣀⣀⣀⣀⣿⣀⣀⣀⣀⣸⣿⣿⣿⣿⣿\n⡄⠀⠀⠀⠀⡈⠛⠟⠛⠛⠉⠉⠉⠉⠙⠛⠛⠋⠉⠉⠉⠉⠙⠛⠛⠟⠉⣉⣿⣿\n⣿⣶⣶⣶⣿⣿⣷⡄⠀⠈⠛⣦⠀⠀⠀⠀⠀⠀⠀⠀⠀⡞⠋⣀⠀⣴⣿⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⣿⡇⠀⠛⠛⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⠚⠃⠀⣿⣿⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⣿⡷⠶⠶⠶⠶⣶⡶⠶⠶⠶⠶⠶⢶⡶⠶⠶⠶⠶⣿⣿⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⠿⠁⣠⠶⢦⡄⢹⡇⢀⡴⠶⣦⠀⢸⡇⢠⠶⠶⡄⠙⢿⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⡀⠀⢧⣀⣠⠇⢸⠇⠸⣄⣀⡼⠀⢸⡇⠸⣄⣠⡟⠀⣸⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⡿⠀⢀⣭⣅⠀⣸⠀⠀⣨⣭⣀⠀⢸⡇⠀⣠⣤⡀⠀⣿⣿⣿⣿\n⣿⣿⣿⣿⣿⣏⠀⠀⡏⠀⢸⡇⣿⠀⢰⡋⠀⣹⠀⢸⡇⢸⡁⠀⣻⠀⠈⣿⣿⣿\n⣿⣿⣿⣿⣿⣿⡆⠀⠉⠛⠋⠀⣿⠀⠀⠙⠛⠉⠀⢸⡇⠀⠙⠛⠁⠀⣾⣿⣿⣿\n⣿⣿⣿⣿⣿⡏⠀⢠⡞⠉⢳⡀⣿⠀⢠⠟⠉⢳⠀⢸⡇⢰⡟⠉⢳⠀⠈⢻⣿⣿\n⣿⣿⣿⣿⣿⣧⡀⠈⠳⠤⠞⠀⣿⠀⠈⠳⠴⠟⠀⢸⡇⠘⠷⠴⠞⠀⢠⣿⣿⣿\n⣿⣿⣿⣿⣿⠟⠀⢠⡴⠲⣆⠀⣿⠀⢀⡴⠲⣦⠀⢸⡇⢀⡴⠖⢦⡀⠈⢻⣿⣿\n⣿⣿⣿⣿⣿⡄⠀⠸⣆⣀⡾⠀⣿⠀⠘⣦⣀⡼⠀⢸⡇⠘⢧⣀⡼⠃⠀⣸⣿⣿\n⣿⣿⣿⣿⣿⣧⣤⣤⣤⣤⣤⣤⣿⣤⣤⣤⣤⣤⣤⣼⣧⣤⣄⣀⣀⣤⣤⣿⣿⣿\n⣿⣿⡿⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙\n⣿⣿⣷⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠\n""")
        
    def retrieve(self):
        return pickle.loads(self.store)


class memory_pickle:
    """Send data to Computers RAM for storage and retrieval later"""
    
    def __init__(self):
        self.store = None

    def send(self, data):
        self.store = pickle.dumps(data)   

    def retrieve(self):
        return pickle.loads(self.store)



class pickle_testing_file:
    """Used for pickling a files to a local testing.pkl\n
    
    Use the following codes\n
    
    from PHHaB_Shared import pickle_testing_file\n
    t = pickle_testing_file()\n
    t.send(Var to be pickled)\n
    
    returndata = t.retrieve()\n
    """
    
    def __init__(self):
        self.file_location = 'Database/testing.pkl'
    
    def send(self, data):
        if os.path.exists(self.file_location):
            os.remove(self.file_location)
        
        # source, destination
        dbfile = open(self.file_location, 'ab')
        pickle.dump(data, dbfile)                    
        dbfile.close()
        
        
    def retrieve(self):
        # for reading also binary mode is important
        dbfile = open(self.file_location, 'rb')    
        db = pickle.load(dbfile)
    
        dbfile.close()
        
        return db
        
      