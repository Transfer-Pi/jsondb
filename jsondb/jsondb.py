import os
import sys

from shutil import rmtree,move,copy
from json import loads,dumps

import logger


class JSON(object):
    """
    Helper Class For Mapping JSON vars to Objects
    """
    def __init__(self,data=dict(),level=1):
        self._level = level
        for key in data:
            if type(data[key]) == dict:
                self.__dict__[key] = JSON(data[key],level=self._level+1)
            else:
                self.__dict__[key] = data[key]

    def __repr__(self):
        return (
                "JSON({\n"
                +'\n'.join([f"{' '*4*self._level}{i} : {self.__dict__[i].__repr__()}" for i in self.__dict__ if not i.startswith("_")])
                +"\n"
                +(' '*4*(self._level-1))+"})"
            )
    
    def __str__(self):
        return self.__repr__()
    
    def __getitem__(self,key):
        return self.__dict__[key]
    
    def __iter__(self):
        for key in self.__dict__:
            if type(self.__dict__[key]) == JSON:
                yield key, self.__dict__[key]()
            else:
                yield key, self.__dict__[key]
    
    def __setitem__(self,key:str,value):
        self.__dict__[key] = value
        
    def __call__(self,):
        return {i:j for i,j in  self.__iter__() if not i.startswith("_")}
        
    def __delitem__(self,key):
        del self.__dict__[key]
                
    def __len__(self,)->int:
        return len(self())

    def __bool__(self,):
        return len(self) > 0

    def keys(self,):
        return self.__dict__.keys()


        
class Record(JSON):
    """
    Record Object For Individual Records
    """
    def __init__(self,path:str=None,**kwargs):
        super().__init__(data=kwargs)
        self._path = path
        
    def __repr__(self,):
        return (
                "Record({\n"
                +'\n'.join([f"{' '*4}{i} = {self.__dict__[i].__repr__()}," for i in self.__dict__ if not i.startswith("_")])
                +"\n"
                +(' '*4*(self._level-1))+"})"
            )
    
    def __str__(self):
        return self.__repr__()
        
    def write(self,):
        try:
            return f"Wrote {open(self._path,'w+').write(dumps(self()))} Chars Successfully"
        except:
            return f"Error Occured While Writing {self._path}"

    def destroy(self,):
        try:
            os.remove(self._path)
            return 1
        except:
            return 0


class Schema:
    
    def __init__(self,path:str):
        pass

    def createUser(self,)->dict:
        pass

    def updateUser(self,)->dict:
        pass
    
    def removeUser(self,)->dict:
        pass

    def authUser(self,)->dict:
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
    
    def __getitem__(self,key)->Record:
        path = os.path.join(self._path,f"{key}.json")
        if os.path.isfile(path):
            return Record(path)
        return JSON()
    
    def insertOne(self,record)->dict:
        assert self._primary_key in record.keys(),f"Record Does Not Contain Primary Key, In this case \"{self._primary_key}\""
        assert isinstance(record,Record), "Record should be of object of jsondb.Record"
        path = os.path.join(self._path,f"{record[self._primary_key]}.json")
        if os.path.isfile(path):
            return {
                "status":False,
                "message":f"{self._primary_key} Already Exists."
            }
        if open(path,'w+').write(dumps(record())):
            return {
                "status":True,
                "message":"Record Inserted Succesfully."
            }
        else:
            return {
                "status":False,
                "message":"Error Inserting Record.."
            }

    

    def deleteOne(self,key:str)->int:
        path = os.path.join(self._path,f"{key}.json")
        if os.path.isfile(path):
            try:
                os.remove(path)
                return 1
            except:
                return 2
        return 0
    
    def deleteAll(self,)->int:
        try:
            os.system(f"rm {self._path}/*.json")
            return 1
        except:
            return 0

    def updateOne(self,key:str,update:dict):
        pass

    def fetchKeys(self,)->list:
        return [i.split(".json")[0] for i in os.listdir(self._path) if i != 'primary_key']
    
    def fetchAll(self,start:int=0,end:int=-1)->list:
        return [
                loads(open(os.path.join(self._path,i)).read())
                for i in os.listdir(self._path) 
                if i.endswith(".json")
            ]

    def fetchOne(self,key:str)->Record:
        path = os.path.join(self._path,f"{key}.json")
        try:
            return Record(path=path,**loads(open(path,"r").read()))
        except:
            return Record()


query_keywords = ['create','delete','update']      

class Database(object):
    def __init__(self,name:str,path:str,logger:logger.Logger=logger.Logger()):
        self._name = name
        self._path = os.path.join(path,name)
        self._logger = logger
        
        for table in os.listdir(self._path):
            if table not in query_keywords:
                self.__dict__[table] = Table(table,os.path.join(self._path,table))
        
    def __repr__(self):
        return f"Database @ {self._path}"
    
    def __getitem__(self,key)->Table:
        return self.__dict__[key]


    def listTables(self,):
        col,_ = os.get_terminal_size()
        print (col*'-')
        print (f"| Table{' '*12}| Path{' '*(col-26)}|")
        print (col*'-')
        for i in self.__dict__:
            if not i.startswith("_"):
                f = f"| {i}{(17-len(i))*' '}| {self.__dict__[i]}"
                print (f+(' '*(col-1-len(f)))+'|')
        print (col*'-')
    
    def createTable(self,name,primary_key):
        path = os.path.join(self._path,name)
        if os.path.isdir(path):
            self._logger.warning(f"Table {name} Already Exists.")
        else:
            os.mkdir(path)
            open(os.path.join(path,"primary_key"),"w+").write(primary_key)
            self.__dict__[name] = Table(name,path)
            self._logger.success(f"Table {name} Created Succesfully.")
    
    def deleteTable(self,name:str):
        path = os.path.join(self._path,name)
        if not os.path.isdir(path):
            self._logger.error(f"Table {name} Does Not Exist.")
        else:
            rmtree(path)
            del self.__dict__[name]
            self._logger.success(f"Table {name} Deleted Succesfully.")
    
    def renameTable(self,old:str,new:str):
        path = os.path.join(self._path,old)
        if not os.path.isdir(path):
            self._logger.error(f"Table {old} Does Not Exist.")
        else:
            path_ = os.path.join(self._path,new)
            move(path,path_)
            del self.__dict__[old]
            self.__dict__[new] = Table(new,path_)
            self._logger.success(f"Table Renamed Succesfully {old} -> {new}.")



class Cursor(object):
    """
    Cursor Class For JSONDB
    """
    def __init__(self,path:str="./",auth:dict={},logger:logger.Logger=logger.Logger()):
        self._path = path
        self._auth = auth
        self._db_path = os.path.join(self._path,"db")
        self._logger = logger
        
        if not os.path.isdir(self._db_path):
            os.mkdir(self._db_path)
            
        for db in os.listdir(self._db_path):
            self.__dict__[db] = Database(db,self._db_path,logger=logger)
            
    def __getitem__(self,key)->Database:
        return self.__dict__[key]

    def __repr__(self)->str:
        return f"Connection @ {self._path}"
            
    def createDB(self,name:str):
        path = os.path.join(self._path,"db",name)
        if not os.path.isdir(path):
            os.mkdir(path)
            self.__dict__[name] = Database(name,self._db_path)
            self._logger.success(f"Database {name} Created Succesfully.")
        else:
            self._logger.error(f"Database {name} Already Exists.")

    def listDB(self,):
        col,_ = os.get_terminal_size()
        print (col*'-')
        print (f"| Database{' '*9}| Path{' '*(col-26)}|")
        print (col*'-')
        for i in self.__dict__:
            if not i.startswith("_"):
                f = f"| {i}{(17-len(i))*' '}| {self.__dict__[i]}"
                print (f+(' '*(col-1-len(f)))+'|')
        print (col*'-')

    def deleteDB(self,name:str):
        path = os.path.join(self._path,"db",name)
        if not os.path.isdir(path):
            self._logger.error(f"Database {name} Does Not Exist.")
        else:
            rmtree(path)
            del self.__dict__[name]
            self._logger.success(f"Database {name} Deleted Succesfully.")




def connect(path:str="./",auth:dict={})->Cursor:
    path = os.path.abspath(path)
    if not os.path.isdir(path):
        os.mkdir(path=path)
        open(os.path.join(path,"config.json"),"w+")
    return Cursor(path=path,auth=auth)


