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
        initializes the site finder
        """
        self.datafetcher = DataFetcher()

    def find_sites(self, param, state, byear, eyear=None):
        """
        Finds sites for a certain parameter within the range of byear to eyear

        Parameters:
            param: String -- name of the parameter we're searching for 
            state: String -- code for the state
            bdate: int -- first year we're collecting data for
            edate: int -- last year we're collecting data for if there is one

        Returns:
            A dataframe with all the sites with valid data for the time range
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

        # print(df)
        df[param] = True

        return df[['site_number', 'county_code', param]]
    
    def best_sites_state(self, state, byear, eyear=None, other_params=[]):
        """
        Finds all sites with potential data for ozone, pm2.5, and any other parameters

        Parameters:
            state: String -- state code
            byear: int -- first year
            eyear: int -- last year (if there is one)
            other_params: [String] -- list of parameters we want besides ozone and pm2.5 (names)

        Returns:
            A dataframe with all valid sites. 
            Every parameter as a 1 (meaning there is information for that time range) or a 0 (no information)
        """
        # first combines list with ozone and PM2.5
        # since we only are doing ozone rn
        dfs = self.find_sites('Ozone', state, byear, eyear)
        params = ['PM2.5 - Local Conditions'] + other_params
        param_dict = {'Ozone' : 'sum'}

        # loops through all the parameters
        for param in params:
            #find_sites('Ozone', '06', 1980, 2020)
            df = self.find_sites(param, state, byear, eyear)
            dfs = pd.concat([dfs, df]).fillna(value=False)
            param_dict[param] = 'sum'

        mini_func = {'county_code': 'first'}
        aggregation_functions = {**mini_func, **param_dict}
        dfs = dfs.groupby(dfs['site_number']).aggregate(aggregation_functions)

        all_params = ['Ozone'] + params
        dfs = dfs.sort_values(by=all_params, ascending=False)

        return dfs

    
