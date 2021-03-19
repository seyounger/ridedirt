





class SKModeler(myLogger):
    def __init__(self,):
        myLogger.__init__(self,name='site_model.log') 
        
    def createModelRun(self,predictor_df,target_df):
        x_df,y_df=self.alignTargetAndPredictorDFs(predictor_df,target_df)
        
    
    
    def alignTargetAndPredictorDFs(self,predictor_df,target_df):
        return predictor_df.align(target_df,join='inner',axis=0)