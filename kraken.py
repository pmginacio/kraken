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
# allow offloading the message processing part to other cores, and/or add multiple 
# coroutines to process the incoming messages.
#
# I did not put significant amount of effort in handling and mitigating
# possible errors and exceptions. I did not put effort in additional
# functionalities or make nice output.
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
# the global counter.
#
# Possible misunderstandings from my side:
#   - price update only issued when new ask/bid were lower/higher than the 
#     lowest/highest ask/bid in the top ten 10. I'm not sure this makes sense
#     since then updates will become less and less frequent?
#   - i was not sure which timestamp to use in the output or whether I should 
#     generate one. therefore I just set it to None.
#   - i assumed that the concept of asset, which I believe is a (coin/market) pair 
#     (e.g. 'ETH/JPY') is a relevant concept to hold on to. that is why i created
#     an Asset() class.
###
import asyncio
import websockets as ws
import json

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

        # use all assests if None was specified
        if lassets is None:
            lassets = self.get_assets()
        else:
            # check if requested assets are valid
            for x in lassets:
                if x not in self.lassets:
                    raise ValueError('%x not available in %s exchange' % (x,self.NAME))

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
            #   snapshot sent once when subscribed
            #   update sent afterwards
            # both are lists with a dictionary in the 1st index
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

                elif len(upd) == 4 and any(x in upd[1] for x in ['a','b']):
                    # 
                    newask = upd[1].pop('a',None)
                    newbid = upd[1].pop('b',None)
                    # logger.debug('sending update: ', upd)
                    await self.lassets[asset_name].update(self.NAME,newask,newbid)
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

        # type conversion, only keep price and volume
        self.lask = [[float(x) for x in y[0:2]] for y in ba]
        self.lbid = [[float(x) for x in y[0:2]] for y in bs]

        self.last_ask = None
        self.last_bid = None
        # not being used 
        self.ts = None

        self.uq = uq

    async def update(self, xch, na, nb):
        '''
        update this asset
        xch is the name of the exchange pushing an update
        na is a new ask in the form [x,y,z[,'r'] or multiple asks in the form
        [[x1,y1,z1[,'r'],[x1,y1,z1[,'r']]
        nb is one or multiple new bids in similar form

        this function adds na and nb to the list of asks/bids if they are 
        smaller/larger than the existing items in the list, respectively

        if na is lower than all items in self.last_ask or
        nb is higher than self.last_bid then an updated is pushed to the
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

        # dont publish unless price changed
        publish = False

        if na is not None:
            # na and nb sometimes are [a,b,c] and sometimes [[a,b,c],[d,e,f],...]
            # if [a,b,c], then convert to [[a,b,c]]
            # same below for ba
            if not isinstance(na[0],list):
                na = [na]
            # convert to float, only keep price and volume
            na = [[float(x) for x in y[0:2]] for y in na]

            # loop new asks and add to the list of last_asks if 
            # lower than the last one
            for iask in na:
                self.last_ask = iask

                if iask[0] < self.lask[-1][0]:
                    # only publish if the best ask, i.e. lower that 
                    # current best
                    if iask[0] < self.lask[0][0]:
                        publish = True
                    
                    # append and sort list ascending
                    tmp = self.lask+[iask]
                    self.lask = sorted(tmp, key=lambda x: x[0])[0:self.n]

        if nb is not None:
            if not isinstance(nb[0],list):
                nb = [nb]

            # convert to float, keep only price and volume
            nb = [[float(x) for x in y[0:2]] for y in nb]

            # loop new bids and add to the list of last_bids if 
            # larger than the last one
            for ibid in nb:
                if ibid[0] > self.lbid[-1][0]:
                    self.last_bid = nb

                    # publish only if the best bid, i.e. higher that 
                    # current best
                    if ibid[0] > self.lbid[0][0]:
                        publish = True

                    # add, sort descending and keep n
                    aux = self.lbid +[ibid]
                    self.lbid = sorted(aux, key=lambda x: x[0], reverse=True)[0:self.n]

        if publish:
            await self.uq.put([xch, self.coin, self.market, self.lask,
                self.lbid, self.last_ask, self.last_bid, self.ts])
        # #DEBUG
        # else:
        #     logger.debug('got update, no price change: ', 
        #         [xch, self.coin, self.market, self.last_ask, self.last_bid])


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
