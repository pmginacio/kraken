#!/usr/bin/env/python3 
# 
# Subscribe to order book levels for all assets at the Kraken exchange.
#
# To run simply use,
#   python3 kraken.py
# 
###
# Exercise
# - write exchange connector for exchange Kraken that will open web-socket 
#   connection and start receiving order_book data stream. API is available on their web site.
# - check for best ask and best bid price change and put data in queue 
#   only if price has changed.
# - write queue worker that prints out price changes in list format 
#   for every received item (coin-market pair):
# item = [exchange_name, coin_name, market_name, last_bid, last_ask, 
#   ten best asks with volume in format [[1,2], [2,2],...],
#   and ten best bids with volume in format [[1,2],[2,2],...], 
#   timestamp].
# Use this https://docs.kraken.com/websockets/#message-subscribe for order_book stream of depth 10.
#
# Please use web-socket connection and asynchronous methods to receive data from Kraken.
###
# This is my implementation to solve the above exercise. There are multiple python
# implementations for the kraken exchange readily found online. In the spirit of the 
# assignment, I refrained for taking a look at them. Because of that, probably my 
# implementation deviates from some more standard approach.
# 
# I designed the application in order to separate websocket communication
# and message processing. To do that I implemented an internal queue 
# in the exchange class. All messages read from the exchange are pushed to the
# internal queue. Another coroutine is tasked to filter messages from the 
# internal queue and only act on the relevant ones. In principle this should 
# allow offloading the message processing part to other cores, and/or add 
# multiple coroutines to process the incoming messages.
#
# I did not put effort in additional functionalities or make nice output.
# I did not put significant amount of effort in handling and mitigating
# possible errors and exceptions. Particularly, if there is a checksum error,
# the program will raise a ValueError exception and stop. As far as I can tell,
# the book update algorightm is working correctly and this should not happen. 
# On the other hand, to make it more robust, one idea would be to resubscribe
# once a checksum becomes invalid and carry on.
#
# While I was developing this application, I kept in mind that ultimately 
# we would like to connect and digest messages from multiple exchanges.
# I implemented a class KRKXCH() to represent specifically the API of the
# kraken exchange. The KRKXCH object will push price updates to a global 
# update queue. As written, it would be quite straightforward to 
# expand this application to handle multiple exchanges.
# The first step would be to create a general Exchange() class, of which,
# each specific exchange would customize to their specific API.
# Afterwards, a pool of exchanges would be ready to push price updates to
# the global queue.
#
# Possible misunderstandings from my side:
#   - with an update there is may be a new bid or a new ask. if there is any of 
#     those I update the last_bid or last_ask in the Asset object and I also update
#     the timestamp. So in the update output, the published timestamp refers to 
#     either one of the last bid or ask.
#   - i assumed that the concept of asset, which I believe is a (coin/market) pair 
#     (e.g. 'ETH/JPY') is a relevant concept to hold on to. that is why i created
#     an Asset() class.
###
import asyncio
import websockets as ws
import json
from zlib import crc32
from bisect import bisect
from copy import deepcopy

# this is used to fetch the list of assets through the REST API
import requests as rq

# setup debugging
import logging
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

class KRKXCH(object):
    """
    class used to read book subscriptions from the the kraken exchange 
    through websockets
    """
    def __init__(self, uq):
        """
        uq is an external asyncio.Queue() used to push price updates
        from this exchange
        """

        super(KRKXCH, self).__init__()

        self.NAME = 'KRAKEN'
        self.WS_URL='wss://ws.kraken.com'
        self.ASSETS_URL = 'https://api.kraken.com/0/public/AssetPairs'
        self.BOOK_DEPTH = 10

        # list Asset() objects subscribed in this exchange. see below
        self.lassets = dict()

        # uq is an external update queue, iq is an internal queue used
        # to filter relevant messages
        self.uq = uq
        self.iq = asyncio.Queue()

    def get_assets(self):
        '''load assets from the REST API returns a list of assets'''
        
        # query list of assets
        r = rq.get(self.ASSETS_URL)
        aux = r.json()

        # discard some assets which not have 'wsname'
        assets = [x['wsname'] for x in aux['result'].values() 
            if 'wsname' in x]
        return assets


    def get_subscribe_payload(self, lassets=None):
        '''
        returns JSON payload used to subscribe to the selected assets
        lassets is a list with requested asset names
        if None, all the assets in the exchange will be subscribed
        '''

        aux = self.get_assets()
        if lassets is None:
            # use all availanle assests if None was specified
            lassets = aux
        else:
            # check if requested assets are valid
            for x in lassets:
                if x not in aux:
                    raise ValueError('%s not available in %s exchange' % (
                        x,self.NAME))

        # return JSON payload
        return json.dumps({
                        "event": "subscribe",
                        "pair" : lassets,
                        "subscription": {
                            "depth" : self.BOOK_DEPTH,
                            "name": "book"
                        }
                    })

    async def connect_and_subscribe(self):
        '''
        connect to the kraken exchange, subscribe to the list of assets
        and listen to all the subscriptions.
        push all messages immediately to the internal queue self.iq
        '''

        async with ws.connect(self.WS_URL) as krk:

            # get connection status
            logger.debug('connected')
            st = await krk.recv()
            
            # subscribe to assets
            await krk.send(self.get_subscribe_payload())
            
            # listen to the subscriptions
            while True:
                upd = await krk.recv()  
                # logger.debug('got upd', upd)
                await self.iq.put(upd)


    async def filter(self):
        '''
        read the internal queue self.iq, filter relevant updates
        and call the corresponding Asset.update()
        '''    

        while True:
            upd = await self.iq.get()
            upd = json.loads(upd)

            # drop everything except
            #   - snapshot sent once when subscribed: a list with a dictionary 
            #       with fields 'as' and 'bs' in the 1st index
            #   - update sent afterwards: similar, with one or more dictionaries,
            #       with fields, at least one of a or b and c. two valid examples 
            #       follow,
            #       
            #       [360, {
            #           'a': [['56.25000', '81.92801619', '1603445162.315265']]
            #       },
            #       {
            #           'b': [['56.25000', '0.00000000', '1603445162.318280'], ['56.11000', '90.60000000', '1603445162.113424', 'r']], 
            #           'c': '1810513080'
            #       }, 'book-10', 'LTC/USD']
            #       or 
            #       [360, {
            #           'a': [['56.25000', '81.92801619', '1603445162.315265']], 
            #           'b': [['56.25000', '0.00000000', '1603445162.318280'], ['56.11000', '90.60000000', '1603445162.113424', 'r']], 
            #           'c': '1810513080'
            #       }, 'book-10', 'LTC/USD']
            #   
            if isinstance(upd,list) and isinstance(upd[1],dict):

                # get asset name
                asset_name = upd[-1]

                # snapshop x has fields x[1]['as']
                if len(upd) == 4 and all(x in upd[1] for x in ['as','bs']):
                    # use it to create a new asset
                    lask = upd[1]['as']
                    lbid = upd[1]['bs']
                    self.lassets[asset_name] = Asset(asset_name,lask,lbid,
                        self.uq)
                    continue

                elif len(upd) >= 4 and any(x in upd[1] for x in ['a','b']):

                    # logger.debug('upd:',upd)

                    # init
                    na = None
                    nb = None
                    cs = None

                    # loop up to two dictionaries and accumulate 
                    # possibly present keys
                    for x in upd[1:3]:
                        if isinstance(x,dict):
                            na = x.pop('a',na)
                            nb = x.pop('b',nb)
                            cs = x.pop('c',cs)

                    # if na is not None and nb is not None:
                    #     logger.debug('na:',na)
                    #     logger.debug('nb:',nb)
                    #     logger.debug('nc:',cs)

                    await self.lassets[asset_name].update(self.NAME,na,nb,cs)
                    continue

            # logger.debug('dropped', upd)

class Asset(object):
    """
    class to keep track of pricing data for a given asset, possibly
    from multiple exchanges
    it is my understanding that an asset is a "%s/%s" %(coin,market)
    pair
    """
    def __init__(self, name, ba, bs, uq, n=10):
        '''
        name is the name of the asset, which should be in the standard 
        form XXX/YYY
        ba is a ascending list of best asks with [x,y,z] strings with 
        price, volume and timestamp
        bs is a descending list of best bids in the same form.
        uq is an external asyncio.Queue() used to push price updates

        n is the length of the internal list of best asks and best bids
        to keep track
        '''
        super(Asset, self).__init__()
        self.n = n
        self.coin,self.market = name.split('/')

        # keep price and volume
        self.lask = [[x for x in y[0:2]] for y in ba]
        self.lbid = [[x for x in y[0:2]] for y in bs]

        self.last_ask = None
        self.last_bid = None

        self.ts = None

        # external price update queue
        self.uq = uq

    async def update(self, xch, na, nb, cs):
        '''
        update this asset
        xch is the name of the exchange pushing an update
        na is a new ask in the form [x,y,z[,'r'] or multiple asks in the form
        [[x1,y1,z1[,'r'],[x1,y1,z1[,'r']]
        nb is one or multiple new bids in similar form

        this function adds na and nb to the list of asks/bids if they are 
        smaller/larger than the existing items in the list, respectively

        if na is lower than all items in self.lask or
        nb is higher than self.lbid then an updated is pushed to the
        update queue self.uq in the form of a list,
        [xch,           # exchange name
        self.coin,      # coin name
        self.market,    # market name
        self.lask,      # list top n asks
        self.lbid,      # list top n bids
        self.last_ask,  # last ask
        self.last_bid,  # last bid
        self.ts]        # timestamp
        '''

        # *_before and *_after commented code was used for debug
        # may be removed after validation
        if na is not None:

            # lask_before=[x.copy() for x in self.lask]
                       
            publish = self._update_book(na,'ask')

            # lask_after=[x.copy() for x in self.lask]
                 
        if nb is not None:

            # lbid_before=[x.copy() for x in self.lbid]

            publish = self._update_book(nb,'bid')

            # lbid_after=[x.copy() for x in self.lbid]

        if cs is not None and not self._check(cs):

            # print(self.coin,self.market)
            # if na is not None:
            #     logger.debug(' lask before:')
            #     for x in lask_before:
            #         print(x)
            #     logger.debug(' na=',na)
            #     logger.debug(' lask after:')
            #     for x in lask_after:
            #         print(x)

            # if nb is not None:
            #     logger.debug(' lbid before:')
            #     for x in lbid_before:
            #         print(x)
            #     logger.debug(' nb=',nb)
            #     logger.debug(' lbid after:')
            #     for x in lbid_after:
            #         print(x)
            
            # it's not a bad idea to resbuscribe when something goes wrong
            # i avoided doing that because it is important to get the book 
            # update algorithm working correctly
            raise ValueError('incorrect checksum')

        if publish:
            await self.uq.put([xch, self.coin, self.market, self.lask,
                self.lbid, self.last_ask, self.last_bid, self.ts])


    def _check(self,cs):
        '''
        compute the CRC32 checksum of the current book and compare it with
        the input cs
        https://docs.kraken.com/websockets/#book-checksum
        return True if they match and False otherwise
        '''

        # only first ten are used
        n=10

        ss = ''
        # loop asks and then bids
        for z in [self.lask[0:n], self.lbid[0:n]]:
            # flatten list of lists
            tmp = [x for y in z for x in y]
            # remove decimal and leading zeros and concatenate
            ss += ''.join(map(lambda x: x.replace('.','').lstrip('0'),tmp))

        # return test for match
        return int(cs) == crc32(ss.encode())

    def _update_book(self,n,mode):
        '''
        update book with new ask/bid n.
        mode can be 'ask' or 'bid'

        function will modify self.lask or self.lbid according to doc
        https://support.kraken.com/hc/en-us/articles/360027821131-How-to-maintain-a-valid-order-book-

        returns True if an update should be published and False otherwise
        '''

        # check input
        if mode not in ['ask','bid']:
            raise ValueError('incorrect mode')

        # init
        # only publish if top bid/ask price changed
        publish = False 
        # dont change n in the calling function
        n_ = deepcopy(n)
        # select which list to update depending on the mode
        last = None
        if mode == 'ask':
            out = self.lask
        else:
            out = self.lbid

        # loop input n and consume asks/bids in it
        while len(n) > 0:

            # split price value pair
            p,v,t = n.pop(0)[0:3]

            # check if price level already in list
            try:
                idx=[x[0] for x in out].index(p)
            except ValueError:
                # price not in top asks
                idx = None

            if idx is None:

                # discard if volume is 0 and not on the list
                if float(v) == 0:
                    continue

                # new price level, find correct index to insert
                if mode =='ask':
                    idx = bisect([float(x[0]) for x in out],float(p))
                else:
                    idx = bisect([-float(x[0]) for x in out],-float(p))

                # insert at the right place
                out.insert(idx,[p,v])

                # pop last item if list is longer than self.n
                if len(out) > self.n:
                    out.pop()

                # publish if top ask/bid 
                if idx == 0:
                    publish = True
            else:
                if float(v) == 0:
                    # delete price level
                    out.pop(idx)

                    # publish if top price level was deleted
                    if idx == 0:
                        publish = True
                else:
                    # update volume for existing price level
                    out[idx][1] = v

            # keep last price level only if volume is not zero
            if float(v) != 0:
                self.ts = t
                if mode == 'ask':
                    self.last_ask = p
                else:
                    self.last_bid = p

        return publish

async def worker(q):
    '''
    consume price changes placed in queue q and print them
    '''

    while True:
        item = await q.get()
        print(item)


async def main():

    # create a queue to publish price updates
    GLOBAL_PRICE_UPDTS = asyncio.Queue()

    # create the kraken exchange object, pass the global update queue
    krk = KRKXCH(GLOBAL_PRICE_UPDTS)

    # start all three coroutines together
    await asyncio.gather(
        asyncio.create_task(krk.connect_and_subscribe()), 
        asyncio.create_task(krk.filter()),
        asyncio.create_task(worker(GLOBAL_PRICE_UPDTS)),
        )

# this gets executed when the module is called from the command line
if __name__ == '__main__':

    asyncio.run(main())
