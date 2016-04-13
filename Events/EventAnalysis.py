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
    return events

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
        self.stocks = []
        self.events = None
        
    def analyse_stocks(self, stocks, oracle, start=None, end=None):
        '''Look for events in start-end window for the list of stocks
        
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
        self.events = pd.concat(self.events, axis=1, keys=self.stocks)
        