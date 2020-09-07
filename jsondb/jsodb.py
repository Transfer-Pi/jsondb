import os

from shutil import rmtree
from json import loads,dumps
from termcolor import colored

log_types ={
    "msg":colored("[MSG]","green",attrs=['bold']),
    "wrn":colored("[WRN]","yellow",attrs=['bold']),
    "err":colored("[MSG]","red",attrs=['bold']),
}

def log(log_type,msg):
    print (log_types[log_type],msg)

def connect(path="./",auth={}):
    path = os.path.abspath(path)
    if not os.path.isdir(path):
        os.mkdir(path=path)
        open(os.path.join(path,"config.json"),"w+")
    return Cursor(path=path,auth=auth)

class Cursor(object):
    """
    Cursor Class For JSONDB
    """
    def __init__(self,path="./",auth={}):
        self._path = path
        self._auth = auth
        self._dbs = os.path.join(self._path,"db")
        
        if not os.path.isdir(self._dbs):
            os.mkdir(self._dbs)
            
        for db in os.listdir(self._dbs):
            self.__dict__[db] = Database(db,self._dbs)
            
    def __repr__(self):
        return f"Connection @ {self._path}"
            
    def createDB(self,name):
        path = os.path.join(self._path,"db",name)
        if not os.path.isdir(path):
            os.mkdir(path)
            self.__dict__[name] = Database(name,self._dbs)
            log("msg",f"Database {name} Created Succesfully.")
        else:
            log("wrn",f"Database {name} Already Exists.")

    def deleteDB(self,name):
        path = os.path.join(self._path,"db",name)
        if not os.path.isdir(path):
            log("err",f"Database {name} Does Not Exist.")
        else:
            rmtree(path)
            del self.__dict__[name]
            log("msg",f"Database {name} Deleted Succesfully.")

query_keywords = ['create','delete','update']
        
class Database(object):
    def __init__(self,name,path):
        self._name = name
        self._path = os.path.join(path,name)
        
        for table in os.listdir(self._path):
            if table not in query_keywords:
                self.__dict__[table] = Table(table,os.path.join(self._path,table))
        
    def __repr__(self):
        return f"Database @ {self._path}"
    
    def create(self,name,primary_key):
        path = os.path.join(self._path,name)
        if os.path.isdir(path):
            log("wrn",f"Table {name} Already Exists.")
        else:
            os.mkdir(path)
            open(os.path.join(path,"primary_key"),"w+").write(primary_key)
            self.__dict__[name] = Table(name,path)
            log("msg",f"Table {name} Created Succesfully.")
    
    def delete(self,):
        pass
    
    def rename(self,):
        pass

class Table(object):
    """
    Table Wrapper For JSONDB
    """
    def __init__(self,name,path):
        self._name = name
        self._path = path
        self._primary_key = open(os.path.join(path,"primary_key"),"r").read()
    
    def __repr__(self):
        return f"Table @ {self._path}"
    
    def __getitem__(self,key):
        path = os.path.join(self._path,f"{key}.json")
        if os.path.isfile(path):
            return Record(path)
        return JSON()
    
    def insertOne(self,record):
        assert self._primary_key in record.keys(),f"Record Does Not Contain Primary Key, In this case \"{self._primary_key}\""
        path = os.path.join(self._path,f"{record[self._primary_key]}.json")
        if os.path.isfile(path):
            return {
                "status":False,
                "message":f"{self._primary_key} Already Exists."
            }
        if open(path,'w+').write(dumps(record)):
            return {
                "status":True,
                "message":"Record Inserted Succesfully."
            }
        else:
            return {
                "status":False,
                "message":"Error Inserting Record.."
            }
    
    def deleteOne(self,query):
        pass
    
    def updateOne(self,query,value):
        pass
    
    def fetch(self,query=None):
        if query:
            pass
    
class JSON(object):
    """
    Helper Class For Mapping JSON vars to Objects
    """
    def __init__(self,data=dict(),inner=False):
        for key in data:
            if type(data[key]) == dict:
                self.__dict__[key] = JSON(data[key],inner=True)
            else:
                self.__dict__[key] = data[key]

    def __repr__(self):
        return self.__dict__.__str__()
    
    def __str__(self):
        return self.__dict__.__str__()
    
    def __getitem__(self,key):
        return self.__dict__[key]
    
    def __iter__(self):
        for key in self.__dict__:
            if type(self.__dict__[key]) == JSON:
                yield key, self.__dict__[key]()
            else:
                yield key, self.__dict__[key]
        
    def __call__(self,):
        return {i:j for i,j in  self.__iter__() if not i.startswith("_Record")}
    
        
class Record(JSON):
    """
    Record Class
    """
    def __init__(self,path):
        self.__path = path
        super(Record,self).__init__(loads(open(path,"r").read()))
    
    def write(self,):
        open(self.__path,"w+").write(dumps(self.__call__()))
