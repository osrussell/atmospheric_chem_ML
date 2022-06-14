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
CURR_VARS = ["Carbon monoxide", "Nitrogen dioxide (NO2)", "PM2.5 - Local Conditions"] + MET_VARS

MONITORS_BY_STATE = 'monitors/byState'
ANNUAL_DATA_BY_SITE = 'annualData/bySite'

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
            byear: int -- first year we're collecting data for
            eyear: int -- last year we're collecting data for if there is one

        Returns:
            A dataframe with all the sites with valid data for the time range
        """

        #TODO: wind handling!!!

        df = self.datafetcher.get_data(MONITORS_BY_STATE, self.datafetcher.find_code(param), 20180618, 20180618, df = True, nparams={'state':state})

        if df.empty:
            return df

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

        return df[['site_number', 'local_site_name', 'county_code', param]]
    
    def best_sites_state(self, state, byear, eyear=None, mandatory_params=['Ozone'], other_params=CURR_VARS):
        """
        Finds all sites with potential data for ozone, pm2.5, and any other parameters.
        NOTE: Ozone is pulled automatically as the base variable 

        Parameters:
            state: String -- state code
            byear: int -- first year
            eyear: int -- last year (if there is one)
            mandatory_params: [String] -- list of parameters that must be present MUST HAVE AT LEAST 1
            other_params: [String] -- list of parameters we want besides ozone and pm2.5 (names)

        Returns:
            A dataframe with all valid sites, or an empty dataframe if there are no matching sites
            These sites have all mandatory parameters and go from most to least other parameters (with hourly check??)
            Every parameter as a 1 (meaning there is information for that time range) or a 0 (no information)
        """
        # first combines list with ozone and PM2.5
        # since we only are doing ozone rn
        dfs = self.find_sites('Ozone', state, byear, eyear)
        if dfs.empty:
            print(f"No matching sites found for state {state}")
            return dfs

        looking_params = mandatory_params + other_params
        #removes duplicate items
        looking_params = list(set(looking_params))

        if 'Ozone' in looking_params:
            looking_params.remove('Ozone')
        found_params = ['Ozone']

        param_dict = {'Ozone' : 'sum'}
        # loops through all the parameters
        for param in looking_params:
            #find_sites('Ozone', '06', 1980, 2020)
            df = self.find_sites(param, state, byear, eyear)
            # if there are sites no sites for the variable and it's mandatory
            if df.empty:
                if param in mandatory_params:
                    print(f"No matching sites found for state {state}")
                    return df
                else:
                    print(f"No data for {param} in this range")
            else: # only concats if there was information
                dfs = pd.concat([dfs, df]).fillna(value=False)
                param_dict[param] = 'sum'
                found_params.append(param)

        # aggregates all data together
        mini_func = {'local_site_name': 'first', 'county_code': 'first'}
        aggregation_functions = {**mini_func, **param_dict}
        dfs = dfs.groupby(dfs['site_number']).aggregate(aggregation_functions)
        # makes sure only sites with the proper mandatory params are included
        for param in mandatory_params:
            dfs = dfs[dfs[param] == 1]

        # makes sure at least mandatory variables are hourly
        for param in mandatory_params:
            for index, row in dfs.iterrows():
                bdate = str(int(byear)) + '0101'
                annual_df = self.datafetcher.get_data(ANNUAL_DATA_BY_SITE, self.datafetcher.find_code(param), bdate, bdate, df = True, nparams={'state':state, 'county':row[1], 'site':index})
                # if there is no annual data at all
                if annual_df.empty:
                    dfs.drop(labels=[index], axis=0, inplace=True)
                    continue
                annual_df = annual_df[annual_df['sample_duration'] == '1 HOUR']
                if annual_df.empty:
                    dfs.drop(labels=[index], axis=0, inplace=True)
            
            if dfs.empty:
                print(f"No hourly data found for state {state} for mandatory sparameter {param}")
                # returns an empty dataframe 
                return dfs

        # finds sites with most of the important variables
        #axis = 1 is the columns
        dfs['total_params'] = dfs[found_params].sum(axis=1)
        dfs = dfs.sort_values(by='total_params', ascending=False)

        return dfs

    
