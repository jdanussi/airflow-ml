import os
import pandas as pd
import pyarrow

from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

import utils.config_params as config

PATH_LOCAL = config.params["PATH_LOCAL"]
outliers_fraction = float(config.params["outliers_fraction"])

def anomaly(year):

    dfs=[]
    #file_name = f"{year}_03_agg.csv"
    file_name = f"{year}_03_agg.parquet"
    file_path = os.path.join(PATH_LOCAL, file_name)

    if os.path.exists(file_path):
        
        df = pd.read_parquet(file_path)

        """
        df = pd.read_csv(file_path, sep=',', index_col=0)

        for key in df.index.get_level_values(0).unique():
            item = df.loc[[key]]
            #item = df.loc[key,:]
            item.set_index('fl_date', inplace=True)
            
            scaler = StandardScaler()
            np_scaled = scaler.fit_transform(item.values.reshape(-1, 1))
            data = pd.DataFrame(np_scaled)
        
            # train isolation forest
            model =  IsolationForest(contamination=outliers_fraction)
            model.fit(data) 
        
            item['anomaly'] = model.predict(data)
            item['origin']=key
            
            item.reset_index(inplace=True)
            item = item[['origin', 'fl_date', 'mean_dep_delay', 'anomaly']]

            dfs.append(item)
        """

        for origin in df['origin'].unique():
            item = df[df['origin']==origin]
            item.drop(['origin'], axis=1, inplace=True)
            item.set_index('fl_date', inplace=True)

            scaler = StandardScaler()
            np_scaled = scaler.fit_transform(item.values.reshape(-1, 1))
            data = pd.DataFrame(np_scaled)
        
            # train isolation forest
            model =  IsolationForest(contamination=outliers_fraction)
            model.fit(data) 
        
            item['anomaly'] = model.predict(data)
            item['origin']=origin
            
            item.reset_index(inplace=True)
            item = item[['origin', 'fl_date', 'mean_dep_delay', 'anomaly']]

            dfs.append(item)




        df = pd.concat(dfs, axis=0)
        df = df.reset_index()
        return df


if __name__ == "__main__":
    pass
        