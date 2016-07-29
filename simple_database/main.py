from .config import BASE_DB_FILE_PATH
from .exceptions import ValidationError
try:
    import cpickle as pickle
except ImportError:
    import pickle
import os
import sys


def create_database(db_name):
    if type(db_name) != str:
        raise TypeError('Invalid db_name type')
    
    folderpath = os.path.join(BASE_DB_FILE_PATH, db_name)
    if os.path.exists(folderpath):
       raise ValidationError('Database with name "{}" already exists.'\
       .format(db_name))
    
    os.makedirs(folderpath)  
    return Database(folderpath)
    

def connect_database(db_name):
    folderpath = os.path.join(BASE_DB_FILE_PATH, db_name)
    if not os.path.exists(folderpath):
        raise ValidationError('Database with name "{}" does not exist.'\
        .format(db_name))
    
    db = Database(folderpath)
    for file_name in os.listdir(folderpath):
        filepath = os.path.join(folderpath, file_name)
        loaded_table = load_table(filepath)
        setattr(db, loaded_table.name, loaded_table)
        db.table_names.append(loaded_table.name)
    return db
        

def load_table(filepath):
    with open(filepath, 'rb') as f:
        table_data = pickle.load(f)
    return Table(table_data[0], table_data[1], table_data[2], table_data[3])
            

class Database:
    
    def __init__(self, folderpath):
        self.folderpath = folderpath
        self.table_names = []
    
    def create_table(self, name, columns):
        if type(name) != str:
            raise TypeError('Invalid table name type.')
        filepath = os.path.join(self.folderpath, name + '.p')
        new_table = Table(name, filepath, columns)
        setattr(self, name, new_table)
        self.table_names.append(name)
    
    def show_tables(self):
        return self.table_names

    
class Table:
    
    def __init__(self, name, filepath, columns, rows=None):
        self.name = name
        self.filepath = filepath
        self.columns = columns
        if rows is None:
            rows = []
        self.rows = rows
        self.__save()
       
    def insert(self, *args):
        new_row = {}
        if len(args) != len(self.columns):
            raise ValidationError('Invalid amount of fields')
        for i, arg in enumerate(args):
            col_header = self.columns[i]
            if type(arg).__name__ != col_header['type']:
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"'.format(col_header['name'], type(arg).__name__, col_header['type']))
            new_row[col_header['name']] = arg
        new_row = Row(new_row)
        self.rows.append(new_row)
        self.__save()
        
    def __save(self):
        table_data = [self.name, self.filepath, self.columns, self.rows]
    #    if sys.version_info[0] < 3:
    #        protocol_version = 2
    #    elif 3.4 > sys.version_info[0] > 3:
    #        protocol_version = 3
    #    elif 3.3 < sys.version_info[0] <= 3.5:
    #        protocol_version = 4
        with open(self.filepath, 'wb') as f:
            pickle.dump(table_data, f, protocol=pickle.HIGHEST_PROTOCOL)

    def count(self):
        return len(self.rows)
        
    def describe(self):
        return self.columns
        
    def all(self):
       return (row for row in self.rows)
        
    def query(self, **kwargs):
        return (row for row in self.rows for key, val in kwargs.items() if \
        getattr(row, key) == val)
        
class Row:
    
    def __init__(self, items):
        for key in items:
            setattr(self, key, items[key])