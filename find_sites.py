import pandas as pd
import requests
import sys
sys.path.append('/Users/olivia/main/research/atmospheric_chem_ML/chem150')
# it says import can't be resolved but it resolves in a notebook? 
from data_fetcher import DataFetcher
import numpy as np
import pandas as pd

CRITERIA_POLLUTANTS = ["Carbon monoxide", "Nitrogen dioxide (NO2)", "Ozone", "PM2.5 - Local Conditions"]
PAMS = ["Nitric oxide (NO)", "Oxides of nitrogen (NOx)"]
MET_VARS = ["Wind Direction - Resultant", "Wind Direction - Scalar", "Wind Speed - Resultant", "Wind Speed - Scalar", "Outdoor Temperature", "Relative Humidity ", "Solar radiation", "Ultraviolet radiation", "Barometric pressure"] 
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

        df = self.datafetcher.get_data(MONITORS_BY_STATE, self.datafetcher.find_code(param), 20180618, 20180618, df = True, nparams={'state':state})

        if df.empty:
            return df

        # turns start dates into years 
        df["open_date"] = df["open_date"].map(lambda date: int(str(date)[:4])) 
        # sorts so that the earliest ozone collection date is before 1980
        df = df[df["open_date"] < byear]
        df = df.reset_index()

        # finds data within the correct year range
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
        df[param] = True

        return df[['site_number', 'local_site_name', 'county_code', param]]
    
    def best_sites_state(self, state, byear, eyear=None, mandatory_params=['Ozone'], other_params=CURR_VARS, verbose=False):
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

        # starts dictionary for the aggregation function later
        param_dict = {'Ozone' : 'sum'}

        ##### FINDS ALL SITES IN THE STATE FOR THE PARAMETERS LISTED #####
        
        for param in looking_params:
            #find_sites('Ozone', '06', 1980, 2020)
            df = self.find_sites(param, state, byear, eyear)
            # if there are sites no sites for the variable and it's mandatory
            if df.empty:
                if param in mandatory_params:
                    if verbose:
                        print(f"No matching sites found for state {state}")
                    return df
                else:
                    if verbose:
                        print(f"No data for {param} in this range for state {state}")
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
                print(f"No hourly data found for state {state} for mandatory parameter {param}")
                # returns an empty dataframe 
                return dfs

        ##### FOR WIND SPEED AND DIRECTION #####
        # this section combines the wind scalar and wind resultant columns for speed and direction if both columns exist
        # otherwise just renames the column to only wind speed

        temp_cols = dfs.columns.tolist()

        # check if both speeds are there 
        if ('Wind Speed - Resultant' in found_params) or ('Wind Speed - Scalar' in found_params):
            if 'Wind Speed - Resultant' in temp_cols:
                found_params.remove('Wind Speed - Resultant')
                if 'Wind Speed - Scalar' in temp_cols:
                    # combine both with max
                    dfs['Wind Speed'] = dfs[['Wind Speed - Resultant', 'Wind Speed - Scalar']].max(axis=1)
                    dfs.drop(['Wind Speed - Resultant', 'Wind Speed - Scalar'], axis=1, inplace=True)
                    found_params.remove('Wind Speed - Scalar')
                else:
                    dfs = dfs.rename({'Wind Speed - Resultant': 'Wind Speed'}, axis=1)
            elif 'Wind Speed - Scalar' in temp_cols:
                dfs = dfs.rename({'Wind Speed - Scalar': 'Wind Speed'}, axis=1)
                found_params.remove('Wind Speed - Scalar')
            found_params.append('Wind Speed')

        # check if both Directions are there
        if ('Wind Direction - Resultant' in found_params) or ('Wind Direction - Scalar' in found_params):
            if 'Wind Direction - Resultant' in temp_cols:
                found_params.remove('Wind Direction - Resultant')
                if 'Wind Direction - Scalar' in temp_cols:
                    # combine both with max
                    dfs['Wind Direction'] = dfs[['Wind Direction - Resultant', 'Wind Direction - Scalar']].max(axis=1)
                    dfs.drop(['Wind Direction - Resultant', 'Wind Direction - Scalar'], axis=1, inplace=True)
                    found_params.remove('Wind Direction - Scalar')
                else:
                    dfs = dfs.rename({'Wind Direction - Resultant': 'Wind Direction'}, axis=1)
            elif 'Wind Direction - Scalar' in temp_cols:
                dfs = dfs.rename({'Wind Direction - Scalar': 'Wind Direction'}, axis=1)
                found_params.remove('Wind Direction - Scalar')
            found_params.append('Wind Direction')

        ##### REFORMATS THE DATAFRAME TO BE COMBINED WITH THE OTHER STATES #####

        dfs['total_params'] = dfs[found_params].sum(axis=1)

        # moves total params sum to the front of the dataframe
        temp_cols = dfs.columns.tolist()
        length = len(temp_cols)
        last = length - 1
        new_cols = temp_cols[0:2] + temp_cols[last: length] + temp_cols[2:last]
        dfs = dfs[new_cols]

        # sorts the dataframe and resets the index
        dfs = dfs.sort_values(by='total_params', ascending=False)
        dfs.reset_index(inplace=True)

        return dfs


    def best_sites_country(self, byear, eyear=None, mandatory_params=['Ozone'], other_params=CURR_VARS):
        """
        Finds the best sites in the country given a year range and parameters

        Parameters:
            state: String -- state code
            byear: int -- first year
            eyear: int -- last year (if there is one)
            mandatory_params: [String] -- list of parameters that must be present MUST HAVE AT LEAST 1
            other_params: [String] -- list of parameters we want besides ozone and pm2.5 (names)

        Returns:
            A dataframe with all valid sites, or an empty dataframe if there are no matching sites
            These sites have all mandatory parameters and go from most to least other parameters
            Every parameter as a 1 (meaning there is information for that time range) or a 0 (no information)
            The parameter must have hourly data to show up 
        """
        states = self.get_state_codes()
        # states = states.loc[[0, 1, 2, 3]]

        dfs = pd.DataFrame()
        for index, row in states.iterrows():
            df =  self.best_sites_state(str(row['state_code']), byear, eyear, mandatory_params, other_params)

            if not df.empty:
                # weeds out anything with less than 6 features 
                df = df[df['total_params'] >= 6]

                # adds state identifying information
                df.insert(0, 'state_name', row['state_name'])
                df.insert(0, 'state_code', row['state_code'])
                df.insert(0, 'climate_zone', row['climate_zone'])

                dfs = pd.concat([dfs, df], axis=0)

            print(f"Finished state {row['state_name']}")

        dfs.fillna(0, inplace=True)

        return dfs

    def get_state_codes(self):
        """
        Returns a dataframe of the state codes for easy reference outside the site finder

        Returns:
            Dataframe!
        """
        url = "https://aqs.epa.gov/data/api/list/states?email=orussell@g.hmc.edu&key=silverwren95"
        r = requests.get(url=url)
        print(f"{r}")
        data = r.json()['Data']
        df = pd.DataFrame(data)

        df = df.rename({'code': 'state_code'}, axis=1)
        df = df.rename({'value_represented': 'state_name'}, axis=1)
        # drops all non-states
        df = df.drop(df.index[[51,52,53,54,55]])

        # adds climate zone
        for index, row in df.iterrows():
            df.at[index,'climate_zone'] = CLIMATE_ZONES[row['state_name']]

        return df

    # def search_usa(self, year):
    #     """
    #     Returns a dataframe that searches the U.S.A. for good sites


    #     """
    #     # gets the states 
    #     r = requests.get(url='https://aqs.epa.gov/data/api/list/states?email=orussell@g.hmc.edu@aqs.api&key=silverwren95')

    #     row = df_08_oz_00.iloc[[0,1,2,3]].copy()
    #     row['state_number'] = '08'
    #     row['state_name'] = 'Colorado'
    #     row['climate_zone'] = 'Southwest'
    #     df_ozone_2000 = df_ozone_2000.append(row)

CLIMATE_ZONES = {
    'Washington' : 'Northwest',
    'Oregon' : 'Northwest',
    'Idaho' : 'Northwest',
    'California' : 'West',
    'Nevada' : 'West',
    'Utah' : 'Southwest',
    'Colorado' : 'Southwest',
    'Arizona' : 'Southwest',
    'New Mexico' : 'Southwest',
    'Montana' : 'Northern Rockies and Plains',
    'North Dakota' : 'Northern Rockies and Plains',
    'South Dakota' : 'Northern Rockies and Plains',
    'Wyoming' : 'Northern Rockies and Plains',
    'Nebraska' : 'Northern Rockies and Plains',
    'Minnesota' : 'Upper Midwest',
    'Iowa' : 'Upper Midwest',
    'Wisconsin' : 'Upper Midwest',
    'Michigan' : 'Upper Midwest',
    'Kansas' : 'South',
    'Oklahoma' : 'South',
    'Texas' : 'South',
    'Louisiana' : 'South',
    'Arkansas' : 'South',
    'Mississippi' : 'South',
    'Hawaii' : 'N/A',
    'Alaska' : 'N/A',
    'Illinois' : 'Ohio Valley',
    'Missouri' : 'Ohio Valley',
    'Tennessee' : 'Ohio Valley',
    'Kentucky' : 'Ohio Valley',
    'Ohio' : 'Ohio Valley',
    'Indiana' : 'Ohio Valley',
    'West Virginia' : 'Ohio Valley',
    'Alabama' : 'Southeast',
    'Georgia' : 'Southeast',
    'South Carolina' : 'Southeast',
    'North Carolina' : 'Southeast',
    'Virginia' : 'Southeast',
    'District Of Columbia' : 'Southeast',
    'Florida' : 'Southeast',
    'Maryland' : 'Northeast',
    'Pennsylvania' : 'Northeast',
    'Delaware' : 'Northeast', 
    'New Jersey' : 'Northeast',
    'Connecticut' : 'Northeast',
    'Rhode Island' : 'Northeast',
    'Massachusetts' : 'Northeast',
    'Vermont' : 'Northeast',
    'New Hampshire' : 'Northeast',
    'New York' : 'Northeast',
    'Maine' : 'Northeast'
}