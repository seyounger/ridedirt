from db_tool import DBTool #for an easy way to store data in a compressed sqlite database
from mylogger import myLogger #helpful for logging errors from multiprocessing objects in Windows or linux
import pandas as pd


class SiteCollection(myLogger):
    """
    One of these for each park/collection of trails, etc. that share some preditor data (e.g., weather data)
    ::self.collection_data contains a time series of data shared by the sites in the collectin
    ::self.site_data_collection contains the SingleSite objects collected together
    """
    def __init__(self,collection_id):
        myLogger.__init__(self,name='sitecollection.log') 
        self.data_key='weather'
        self.collection_id=collection_id
        self.dbt=DBTool()
        self.dc=DataCollector()
        self.collection_data_db=lambda key:self.dbt.anyNameDB(f'collection_{self.collection_id}',tablename=key) 
        self.site_data_collection=self.dbt.anyNameDB('site_data_collection',tablename=collection_id)
        self.train_dates=[]
        self.predict_dates=[]
    
    def updateTrainDates(self,):
        datelist=[]
        for site_name,site_obj in self.site_data_collection().items():
            datelist.extend(site_obj.site_data['target'].index.get_level_values('Date').unique().to_list())
        self.train_dates=list(dict.from_keys(dates+self.train_dates))#unique values, preserving order
        
    
    
    def updateCollectionData(self,):
        with self.collection_data_db(self.data_key) as c_db:
        
            collected_date_list=c_db['collected_date_list']
            for tdate in self.train_dates:
                if not tdate in collected_date_list:
                    df=DataCollector.collect(tdate,self.collection_id,self.data_key)
                    c_db['collected_date_list'].append(tdate)
                    if not 'data_df' in c_db:
                        c_db['data_df']=df
                    else:
                        c_db['data_df']=pd.concat([c_db['data_df'],df],axis=0)
                    c_db.commit()
                
                
        
    def addSingleSiteData(self,site_name,data,):
        if not site_name in self.site_data_collection().keys():
            self.logger.info(f'adding a new site! site_name:{site_name}')
            s_site=SingleSite(site_name,self.collection_id)
        else:
            s_site=self.site_data_collection()[site_name]
        s_site.updateRecord(data)
        self.saveSingleSite(site_name,s_site)
        
    def updateSiteRecords(self,path):
        df=pd.read_excel(path,index_col=[0,1,2])
        site_name_list=df.index.get_level_values('Trail Name').unique().to_list()
        for site_name in site_name_list:
            self.addSingleSiteData(site_name,df.loc[(slice(None),slice(None),site_name),:])
       
    def

            
    def saveSingleSite(self,site_name,s_site_obj):
        self.dbt.addToDBDict([{site_name:s_site_obj}],self.site_data_collection)
    
    def dumpToHTML(self,path='dump.html'):
        html_str=''
        for site_name,site_obj in self.site_data_collection().items():
            html_str+=f'site_name: {site_name}\n'
            for key,df in site_obj.site_data.items():
                if df is None:
                    new_str='no data'
                else:
                    new_str=df.to_html()
                html_str+=f'site_data: {key}\n'+new_str+'\n\n' # backslash n is new line
            html_str+='\n\n\n\n'
        with open(path,'w') as f:
            f.write(html_str)
            
    
    
        

class SingleSite(myLogger):
    def __init__(self,site_name,collection_id):
        myLogger.__init__(self,name='Destination.log') 
        self.site_name=site_name
        self.collection_id=collection_id
        self.site_data={'target':None,'x':None}#DBTool().anyNameDB('_data',tablename=collection_id) #data shared
        
    def updateRecord(self,data):
        assert type(data) in [pd.DataFrame,pd.Series],f'expecting DF or Seris for data but got: {type(data)}'
        if self.site_data['target'] is None:
            self.site_data['target']=data
        else:
            prev_data=self.site_data['target']
            prev_data_idx=prev_data.index
            new_data_idx=data.index
            just_new_data=data.drop(index=data.index.intersection(prev_data_idx))
            print('just_new_data',just_new_data)
            self.site_data['target']=pd.concat([prev_data,just_new_data],axis=0)
            '''if len(just_new_data)>0:
                self.site_data['target']=pd.concat([prev_data,just_new_data])
            else:
                self.logger.warning(f'no new data to add for site_name:{self.site_name}')'''
     
                
        
        
