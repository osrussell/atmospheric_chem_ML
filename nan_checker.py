import pandas as pd
import matplotlib.pyplot as plt

class NaNChecker():
    """
    Python API to queury from AQS database.
    """
    # hey 

    def __init__(self, data):
        """
        initializes the object with the dataframe you want checked
        """
        # params for checking for Nans if needed
        # self.params = {
        #     'email': "orussell@g.hmc.edu",
        #     'key': "silverwren87"}
        self.df = pd.DataFrame(data=data)

    def nan_summary():
        """
        prints out a summary of all the analyses of nans
        """
        return None

    def tally_nans(self, variable):
        """
        tallys up the spread of how many nans in a row

        Parameters:
            variable: string - name of column we want to check NaNs for
        """
        # creates a dataframe of just that variable
        var_df = self.df[variable]
        var_df_nans = var_df.isnull()
        # loop through all variables, if NaN 
        tally_df = pd.DataFrame({'NaNs':[]})
        tally = 0 # counts NaNs in a row

        for x in var_df_nans:
            if x == True: # so if val is NaN
                tally += 1
            if (x == False) and (tally != 0): # if we switch back to False and also the tally has stuff in it
                tally_df.loc[len(tally_df.index)] = tally # adds tally to the df
                tally = 0

        # NEED TO FIGURE OUT WHERE TO DISPLAY THIS RESULT OR HOW TO USE THIS INFORMATION

        return tally_df.hist()


    def graphNaNTypes(df, dataLabel, timeFrame):
        """
        @param df - the dataframe 
        @param dataLabel - the name of the column of data to analyze (eg "Ozone")
        @param timeFrame - the timeframe to graph by (eg "month")
        @return (graph, nanDictList, numNaNsTotal)
            graph - the graph generated
            nanDictList - the list of dictionaries containing the type of NaN as a key and number of NaNs of that type as the value for each timeframe
            numNaNsTotal - a list of the total number of NaNs in each timeframe
        """
        
        if timeFrame == "year":
            sYear = df.iloc[0].name.year
            eYear = df.iloc[-1].name.year
            xAxis = list(range(sYear, eYear+1))
        elif timeFrame == "season":
            xAxis = ["Jan-Feb-Mar", "Apr-May-Jun", "Jul-Aug-Sep", "Oct-Nov-Dec"]
        elif timeFrame == "month":
            xAxis = ["Jan", "Feb", "March", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        elif timeFrame == "weekday":
            xAxis = ["Mon", "Tues", "Wed", "Thurs", "Fri", "Sat", "Sun"]
        elif timeFrame == "day":
            xAxis = list(range(1, 32))
        elif timeFrame == "hour":
            xAxis = ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', 
                '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00',
                '20:00', '21:00', '22:00', '23:00']
        else:
            pass # throw an error maybe?

        xAxisLen = len(xAxis)
        
        # edited df to contain only NaN values
        nanDF = df[df[dataLabel].isna()]

        # list of dictionaries to easily store data and search by message
        nanDictList = []

        # also keeping parallel arrays so can create histogram
        # parallel arrays len(messageList) == len(valueList)
        # with each message corrosponding to the spot in valueList that holds the list of number of that message for each hour
        messageList = []
        valueList = []
        numNaNsTotal = [0]*xAxisLen

        for xIndex in range(xAxisLen):
            # dataframe for specific hour/day/month/etc
            # currentDF = nanDF.loc[lambda row: row['time_local'] == hours[hourIndex]] # TODO: fix this

            if timeFrame == "year":
                currentDF = nanDF.loc[nanDF.index.year == xIndex + sYear]
            elif timeFrame == "season":
                currentDF = nanDF.loc[(nanDF.index.month >= xIndex*3 + 1) & (nanDF.index.month <= xIndex*3 + 3)]
            elif timeFrame == "month":
                currentDF = nanDF.loc[nanDF.index.month == xIndex + 1]
            elif timeFrame == "weekday":
                currentDF = nanDF.loc[nanDF.index.weekday == xIndex]
            elif timeFrame == "day":
                currentDF = nanDF.loc[nanDF.index.day == xIndex + 1]
            elif timeFrame == "hour":
                currentDF = nanDF.loc[nanDF.index.hour == xIndex]
            else:
                pass # throw an error maybe?


            nanDictList.append({})
            for message in currentDF[dataLabel+' - qualifier']:
                numNaNsTotal[xIndex] += 1

            if message in nanDictList[-1]:
                nanDictList[-1][message] += 1
            else:
                nanDictList[-1][message] = 1

            if message not in messageList:
                valueList.append([0]*xAxisLen)
                messageList.append(message)
        
            valueList[messageList.index(message)][xIndex] += 1

        graph = plt.figure(figsize = [15, 6])

        plt.bar(xAxis, valueList[0])
        bottoms = valueList[0]
        for i in range(1, len(valueList)):
            plt.bar(xAxis, valueList[i], bottom=bottoms)
            bottoms = list(map(lambda x, y: x+y, bottoms, valueList[i]))

        plt.legend(messageList)
    
        return (graph, nanDictList, numNaNsTotal)
        