#!/usr/bin/env/python3 
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
# This is my implementation to solve the above exercise.
# 
#
#
#
#
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
    """docstring for KRKXCH"""
    def __init__(self, uq):

        super(KRKXCH, self).__init__()

        self.NAME = 'KRAKEN'
        self.WS_URL='wss://ws.kraken.com'
        self.ASSETS_URL = 'https://api.kraken.com/0/public/AssetPairs'
        self.BOOK_DEPTH = 10

        self.lassets = dict()

        self.uq = uq
        self.iq = asyncio.Queue()

    def get_assets(self):
        '''load assets from the REST API returns a list of assets'''
        # query list of assets

        r = rq.get(self.ASSETS_URL)
        aux = r.json()

        # some assests do not have 'wsname'
        assets = [x['wsname'] for x in aux['result'].values() 
            if 'wsname' in x]
        return assets


    def get_subscribe_payload(self, lassets=None):
        '''
        returns payload used to subscribe to the selected assets
        lassets is a list with asset names
        if None, all the assets in the exchange will be subscribed
        '''

        # use all assests if None was specified
        if lassets is None:
            lassets = self.get_assets()
        else:
            for x in lassets:
                if x not in self.lassets:
                    raise ValueError('%x not available in %s exchange' % (x,self.NAME))

        # 
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
        and listen to all the subscriptions. push them all to the internal 
        object queue self.iq
        '''

        async with ws.connect(self.WS_URL) as krk:

            # get connection status
            print('connected')
            st = await krk.recv()
            
            # subscribe to assets
            await krk.send(self.get_subscribe_payload())

            # listen to the subscriptions
            while True:
                upd = await krk.recv()  
                # print('got upd', upd)
                await self.iq.put(upd)


    async def filter(self):
        '''
        filter relevant updates and push them to the update queue at 
        self.uq
        '''    

        # 
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
                    # print('sending update: ', upd)
                    await self.lassets[asset_name].update(self.NAME,newask,newbid)
                    continue

            # print('dropped', upd)

class Asset(object):
    """"""
    def __init__(self, name, ba, bs, uq, n=10):
        '''
        todo
        '''
        super(Asset, self).__init__()
        self.n = n
        self.name = name
        # type conversion, only keep price and volume
        self.lask = [[float(x) for x in y[0:2]] for y in ba]
        self.lbid = [[float(x) for x in y[0:2]] for y in bs]

        self.last_ask = None
        self.last_bid = None
        self.ts = None

        self.uq = uq

    async def update(self, xch, na, nb):
        '''
        store newask and newbid 
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

            # add to the list of last n asks if lower than the last one
            if na[0][0] < self.lask[-1][0]:
                self.last_ask = na
                
                # only publish if the best ask, i.e. lower that current best
                if na[0][0] < self.lask[0][0]:
                    publish = True
                
                # add and sort list ascending
                aux = self.lask+na
                self.lask = sorted(aux, key=lambda x: x[0])[0:self.n]
                publish = True

        if nb is not None:
            if not isinstance(nb[0],list):
                nb = [nb]

            # convert to float only keep price and volume
            nb = [[float(x) for x in y[0:2]] for y in nb]

            if nb[0][0] > self.lbid[-1][0]:
                self.last_bid = nb

                # publish only if the best bid, i.e. higher that current best
                if nb[0][0] > self.lbid[0][0]:
                    publish = True

                # add, sort descending and keep n
                aux = self.lbid +nb
                self.lbid = sorted(aux, key=lambda x: x[0], reverse=True)[0:self.n]


        if publish:
            coin_name, market_name = self.name.split('/')
            await self.uq.put([xch, coin_name, market_name, self.lask,
                self.lbid, self.last_ask, self.last_bid, self.ts])
        # #DEBUG
        # else:
        #     coin_name, market_name = self.name.split('/')
        #     print('got update, no price change: ', 
        #         [xch, coin_name, market_name, self.last_ask, self.last_bid])


async def worker(q):
    '''
    consume prices changes placed in queue q and print them
    '''

    while True:
        item = await q.get()
        print(item)


async def main():

    # create a queue to publish price updates
    GLOBAL_PRICE_UPDTS = asyncio.Queue()

    # create the kraker exchange object, pass the global update queue
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
