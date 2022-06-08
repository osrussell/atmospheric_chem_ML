import pandas as pd
import sys
sys.path.append('/Users/olivia/main/research/atmospheric_chem_ML/chem150')
# it says import can't be resolved but it resolves in a notebook? 
from data_fetcher import DataFetcher
import numpy as np
import pandas as pd

CRITERIA_POLLUTANTS = ["Carbon monoxide", "Nitrogen dioxide (NO2)", "Ozone", "PM2.5 - Local Conditions"]
PAMS = ["Nitric oxide (NO)", "Oxides of nitrogen (NOx)"]
MET_VARS = ["Wind Direction - Resultant", "Wind Speed - Resultant", "Outdoor Temperature", "Relative Humidity ", "Solar radiation", "Ultraviolet radiation", "Barometric pressure"] 

MONITORS_BY_STATE = 'monitors/byState'

class findSites():
    """
    Python API to queury from AQS database.
    """

    def __init__(self):
        """
        initializes the object with the dataframe you want checked
        """
        # TODO: has a dataframe with the site codes!!!
        # TODO: also clean up this part
        self.site_codes = []
        self.datafetcher = DataFetcher()

    def find_sites(self, param, state, byear, eyear=None):
        """
        param: String -- name of the parameter we're searching for 
        state: String -- code for the state
        bdate: int -- first year we're collecting data for
        edate: int -- last year we're collecting data for if there is one
        """

        #TODO: wind handling!!!

        df = self.datafetcher.get_data(MONITORS_BY_STATE, self.datafetcher.find_code(param), 20180618, 20180618, df = True, nparams={'state':state})
        # print(df)
        # turns start dates into years 
        df["open_date"] = df["open_date"].map(lambda date: int(str(date)[:4])) 
        # sorts so that the earliest ozone collection date is before 1980
        df = df[df["open_date"] < byear]
        # now makes sure the site is still open
        df = df.reset_index()

        if eyear == None:
            df = df.fillna(value='None')
            df = df[df["close_date"] == 'None']
        else:
            df = df.fillna(value="9999")
            df["close_date"] = df["close_date"].map(lambda date: int(str(date)[:4])) 
            df = df[df["close_date"] > eyear]
            df = df.replace(to_replace=9999, value='None') 

        df = df.drop_duplicates(subset = "site_number")
        df = df.reset_index()

        # adds a column for true/false for the variable 
        # TODO: add a way to see the site name
        df['site_name'] = df['site_number']
        # print(df)
        df[param] = True

        # returns just the site numbers, 
        # return df[['site_number','open_date', 'close_date', param]]
        return df[['site_number', 'site_name', param]]

    def join(self):
        '''
        Processes the array of all the information to return
        '''
        return None
    
    def best_sites_state(self, state, byear, eyear=None, other_params=[]):
        """
        state: String -- state code
        byear: int -- first year
        eyear: int -- last year (if there is one)
        other_params: [String] -- list of parameters we want besides ozone and pm2.5 (names)
        """
        # first combines list with ozone and PM2.5
        # since we only are doing ozone rn
        dfs = self.find_sites('Ozone', state, byear, eyear)
        params = ['PM2.5 - Local Conditions'] + other_params

        # loops through all the parameters
        for param in params:
            #find_sites('Ozone', '06', 1980, 2020)
            df = self.find_sites(param, state, byear, eyear)

        dfs = pd.concat([dfs, df]).fillna(value=False)
        dfs = dfs.groupby(dfs['site_number']).sum()
        # reformats the data frame

        return dfs
