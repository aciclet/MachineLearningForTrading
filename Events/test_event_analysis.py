'''Test our event analysis class
'''
import unittest

from EventAnalysis import EventAnalysis

class EventTest(unittest.TestCase):
    '''Main tests'''
    
    def test_event_seach(self):
        '''Test the event seach feature'''
        evt = EventAnalysis()
        stocks = ['AAPL', 'XOM', 'IVV']
        
        def oracle(df):
            return df.Close > df.Open
        
        print evt.analyse_stocks(stocks, oracle)


if __name__ == '__main__':
    unittest.main()