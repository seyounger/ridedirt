from db_tool import DBTool #for an easy way to store data in a compressed sqlite database
from mylogger import myLogger #helpful for logging errors from multiprocessing objects in Windows or linux



class DestinationCollection(myLogger):
    """
    One of these for each park/collection of trails, etc. that share some preditor data (e.g., weather data)
    ::self.dc_data contains a time series of data for the destination
    """
    def __init__(self,collection_id):
        myLogger.__init__(name='Destination.log') 
        self.collection_id=collection_id
        self.dbt=DBTool()
        self.dc_data=self.dbt.anynameDB('dc_data',tablename=collection_id) #data shared
        self.site_data_collection=self.dbt.anynameDB('site_data_collection',tablename=collection_id)
        
    def newSingleSite(self,site_name):
        assert not site_name in self.site_data_collection().keys(),f'site_name:{site_name} already exists in {self.collection_id}'
        s_site=SingleSite(site_name,self.collection_id)
        self.dbt.addToDBDict([{site_name:s_site}],self.site_data_collection)
        
    def addSingleSiteData(self,site_name,data,):
        s_site=self.site_data_collection()[site_name]
        s_site.add_data(data)
        

class SingleSite(myLogger):
    def __init__(self,site_name,collection_id):
        myLogger.__init__(name='Destination.log') 
        self.site_name=site_name
        self.collection_id=collection_id
        self.site_data={'y':None,'x':None}#DBTool().anynameDB('_data',tablename=collection_id) #data shared
        
    def addData(self,data):
        assert type(data) is dict,f'expecting dict for data but got: {type(data)}'
        for key,val in data.items():
            self.site_data[key]=self.update_data(self.site_data[key],val)
            
    def update_data(self,old_data,new_data):
        
            if not self.site_data[key] is None:
                self.site_data[key]
                
        
        
