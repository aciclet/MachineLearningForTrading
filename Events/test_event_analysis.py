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
        
        evt.analyse_stocks(stocks, oracle)
        
        evt.analyse_events(window=10)

        changes = evt.average_change()

        self.assertEqual(changes.iloc[10], 1)

if __name__ == '__main__':
    unittest.main()