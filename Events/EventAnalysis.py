'''Class to analyse and find events.

This libraries exposes classes to find and work with events in stock data.
'''
import pandas as pd
import scipy as sp
import multiprocessing as mlt
import datetime
from dateutil import parser
from functools import partial
import os

# Windows multiprocessing API is broken
if os.name == 'posix':
    from multiprocessing import Pool
else:
    from multiprocessing.pool import ThreadPool as Pool
    
# Just in case I update to pandas 18.0
try:
    import pandas_datareader.data as web
except ImportError:
    import pandas.io.data as web


def _process_one(ticker, func=None, start=None, end=None):
    '''Process one ticker. This will be heavily parallelized.'''
    data   = web.DataReader(ticker, 'yahoo', start, end)
    events = func(data)
    return (ticker, events, data)


class EventAnalysis(object):
    '''Main class to analyse events
    
    Attributes
    ----------
    stocks: iterable
        
    events: DataFrame
        Dataframe of boolean, rownames are dates and columns are tickers. A 
        True value would represent an event
    
    Methods
    -------
    analyse_stocks(stocks, oracle, start=None, end=None)
        Fetch data for all the tickers in stocks between the end and start
        dates and uses the oracle to find events.
        
    analyse_events(window=20)
        Computes summary statistics about each events and its next <window> days
        
    '''
    
    def __init__(self):
        '''Build a base object.
        '''
        self.stocks   = []   # List of stock used
        self.events   = None # DataFrame of booleans
        self.analysis = None # One column by event
        self.params   = {}   # Params of the analysis
        
    def analyse_stocks(self, stocks, oracle, start=None, end=None):
        '''Look for events in start-end window for the list of stocks.
        
        Parameters
        ----------
        stocks: iterable
            List of tickers that should be processed
        oracle: function
            A function that should take as input a dataframe and 
            return a series of Boolean with a True value if the day should
            be considered as an event 
        start, end: str
            Start/end date
        '''
        start = parser.parse(start or '2006-01-01')
        end   = parser.parse(end or '2016-01-01')
        self.stocks = stocks
        
        process = partial(_process_one, func=oracle, start=start, end=end)
        pool    = Pool(processes=mlt.cpu_count())
        events  = pool.map(process, stocks)
        # DataFrame of events
        self.events = pd.concat([e[1] for e in events],
                                 axis=1, keys=self.stocks)
        # Dataframe of close prices              
        self.close  = pd.concat([e[2].Close for e in events],
                                axis=1, keys=self.stocks)
        
        
    def analyse_events(self, window=20):
        '''Find events and summarize their mouvements on the next <window day>.
        
        Parameters
        ----------
        window: int
            Number of days to look after the event
        '''
        close  = self.close
        events = self.events
        
        def event_value(ticker):
            evt_list = []
            for idx, is_event in enumerate(events[ticker]): 
                if not is_event: continue
                # if event look at the -10 +<window> days around the event
                val_range = range(idx - 10, idx + window + 1)
                try:
                    evt_list.append(close[ticker].iloc[val_range].values)
                except IndexError:
                    pass # We do not want to add event on the border

            colnames = ["%s_%d"%(ticker, i) for i in range(len(evt_list))]
            # Returns a dataframe with 10 + window columns 
            out = pd.DataFrame(data=zip(*evt_list), columns=colnames)
            return out
            
        pool = Pool(processes=mlt.cpu_count())
        data = pool.map(event_value, self.stocks)
        self.analysis = data = pd.concat(data, axis=1)
        self.params = {'window': window, 'look_back': 10}
        
        
    def average_change(self, ticker=None):
        '''Returns the avarage returns.'''
        if ticker is None:
            data = self.analysis
        val = data.divide(data.loc[self.params['look_back'], :], axis=1)
        return val.mean(axis=1)
        