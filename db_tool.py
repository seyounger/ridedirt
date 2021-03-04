from sqlitedict import SqliteDict
from shutil import copy
import sys,os,logging
from mylogger import myLogger
import zlib, pickle, sqlite3
from time import sleep

class DBTool():
    def __init__(self):
        func_name='DBTool'
        self.logger=logging.getLogger()
        data_dir=os.path.join(os.getcwd(),'data')
        self.data_dir=data_dir
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
            
    
    def anyNameDB(self,dbname,tablename='data'): #returns a callable that can be used multiple times to access the database
        name=dbname+'.sqlite'
        path=os.path.join(self.data_dir,name)
        return lambda: SqliteDict(
            filename=path,tablename=tablename,
            encode=self.my_encode,decode=self.my_decode)
   
    
    def addToDBDict(self,save_list,db): #robust updating of sqlite DB
        try:
            if type(save_list) is dict:
                save_list=[save_list]
            
            saved=False;tries=0
            while not saved:
                try:
                    with db() as dbdict:
                        for dict_i in save_list:
                            for key,val in dict_i.items():
                                if key in dbdict:
                                    self.logger.warning(f'overwriting val:{dbdict[key]} for key:{key}')
                                    dbdict[key]=val
                                else:
                                    dbdict[key]=val
                        dbdict.commit() #write to disk
                        saved=True
                except:
                    tries+=1
                    self.logger.exception(f'dbtool addtoDBDict error! tries:{tries}')
                    sleep(2)
                    if tries>100:
                        self.logger.warning(f'abandoning save_list:{save_list}')
                        break
                
            return  
        except:
            self.logger.exception(f'addToDBDict outer catch')

        
    def my_encode(self,obj):#compresses whatever is put into the database
        return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL),level=9))
    
    def my_decode(self,obj):#decompresses whatever comes from the database
        return pickle.loads(zlib.decompress(bytes(obj)))
    #mydict = SqliteDict('./my_db.sqlite', encode=self.my_encode, decode=self.my_decode)
            
            
    ####################################################            
    def re_encode(self,path): #for changing compression level of existing non-compressed sqlitedict
        assert os.path.exists(path),f'{path} does not exist'
        bpath=path+'_backup'
        os.rename(path,bpath)
        #os.remove(path)
        tablenames=SqliteDict.get_tablenames(bpath)
        for name in tablenames:
            self.logger.info(f'starting tablename:{name}')
            with SqliteDict(filename=bpath,tablename=name,) as olddict:
                with SqliteDict(filename=path,tablename=name,encode=self.my_encode, decode=self.my_decode) as newdict:
                    keys=list(olddict.keys())
                    kcount=len(keys)
                    self.logger.info(f'for tablename:{name}, keys: {keys}')
                    for k,key in enumerate(keys):
                        if (k+1)%100==0:
                            print(f'{k}/{kcount}.',end='')
                        val=olddict[key]
                        newdict[key]=val
                        newdict.commit()
         
        
        
  
            
            