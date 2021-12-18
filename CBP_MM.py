#   Crypto MM
#   Market Maker algo for BTC-GBP on Coinbase Pro
#   01/09/2018 


from gdax.websocket_client import WebsocketClient
from requests.exceptions import RequestException
from gdax.public_client import PublicClient
from websocket import create_connection
from base64 import b64encode, b64decode
from requests.auth import AuthBase
from threading import Thread, Lock
from bintrees import FastRBTree
from datetime import datetime
from time import clock, sleep
from json import dumps, loads
from getpass import getpass
from hashlib import sha256
from uuid import uuid4
from csv import writer
from hmac import new
from sys import argv
import numpy as np
import simplefix
import requests
import socket
import time

nhup = False
if len(argv) > 1:
    if argv[1] == 'nohup':
        nhup = True
lock = Lock()

def command_line(api):
    print('\nCommand line options: exit, best bid, best ask, BTC balance')
    while True:
        instruction = input("")
        if instruction == "exit":
            break
        elif instruction == "best bid":
            print(api.best_bid)
        elif instruction == "best ask":
            print(api.best_ask)
        elif instruction == "bid depth":
            print(api.bid_depth)
        elif instruction == "ask depth":
            print(api.ask_depth)
        elif instruction == "BTC balance":
            print(api.BTC)


class CoinbaseExchangeAuth(AuthBase):
    def __init__(self):
        self.api_key = ""
        self.api_sec = ""
        self.api_pwd = ""

    def __call__(self, request):
        timestamp = str(time.time())
        message   = timestamp + request.method + request.path_url
        if request.body:
            message += request.body
        message   = bytes(message, 'utf-8')
        hmac_key  = b64decode(self.api_sec)
        signature = new(hmac_key, message, sha256)
        sign_b64  = b64encode(signature.digest())

        request.headers.update({
            'CB-ACCESS-SIGN': sign_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.api_pwd,
            'Content-Type': 'application/json'
        })
        return request


class OrderBook():
    
    def __init__(self, product_id=''):
        self.product_id      = product_id
        self._asks           = FastRBTree()
        self._bids           = FastRBTree()
        self.best_ask        = 0
        self.best_bid        = 0
        self.spread          = 0
        self.ask_depth       = 0
        self.bid_depth       = 0
        self.ask_vol         = 0
        self.bid_vol         = 0
        self._client         = PublicClient()
        self._sequence       = -1
        self._current_ticker = None
        self.n = 0
        
    def log_vol(self, data):
        with open('cbp_trade_log.csv', 'a') as csv_f:
            csv_w = writer(csv_f, lineterminator='\n')
            csv_w.writerow(data)

    @property
    def full_channel(self): 
        sub = dumps({"type": "subscribe","product_ids": [self.product_id],"channels": ['full']})
        ws  = create_connection(self.wss_url)
        ws.send(sub)
        ws.recv()
        self.connected = True

        while True:
            try:
                msg = loads(ws.recv())
                self.got_level2 = 1
            except Exception:
                try:
                    self.got_level2 = 0
                    sleep(4)
                    ws = create_connection(self.wss_url)
                    ws.send(sub)
                    ws.recv()
                except Exception:
                    continue
                                        
            self.on_message(msg)
            #print(self.best_ask, self.best_bid, end='\r')

            if int(clock()) % 30 == 0:
                try:
                    ws.ping("keepalive")
                except Exception:
                    continue

    def reset_book(self):
        self._asks = FastRBTree()
        self._bids = FastRBTree()
        res = self._client.get_product_order_book(product_id=self.product_id, level=3)
        for bid in res['bids']:
            self.add({
                'id': bid[2],
                'side': 'buy',
                'price': float(bid[0]),
                'size': float(bid[1])
            })
        for ask in res['asks']:
            self.add({
                'id': ask[2],
                'side': 'sell',
                'price': float(ask[0]),
                'size': float(ask[1])
            })
        self._sequence = res['sequence']
        self.best_bid  = self.get_bid()
        self.best_ask  = self.get_ask()
        self.spread    = self.best_ask - self.best_bid
        self.bid_depth = sum([b['size'] for b in self.get_bids(self.best_bid)])
        self.ask_depth = sum([b['size'] for b in self.get_asks(self.best_ask)])

    def on_message(self, message):

        sequence = message['sequence']
        if self._sequence == -1:
            self.reset_book()
            return
        if sequence <= self._sequence:
            # ignore older messages (e.g. before order book initialization from getProductOrderBook)
            return
        elif sequence > self._sequence + 1:
            self.on_sequence_gap(self._sequence, sequence)
            return

        msg_type = message['type']
        if msg_type == 'open':
            self.add(message)
        elif msg_type == 'done' and 'price' in message:
            self.remove(message)
        elif msg_type == 'match':
            self.match(message)
            self._current_ticker = message
        elif msg_type == 'change':
            self.change(message)
        self._sequence = sequence
        
        self.best_bid = self.get_bid()
        self.best_ask = self.get_ask()
        self.spread   = self.best_ask - self.best_bid
        self.bid_depth = sum([b['size'] for b in self.get_bids(self.best_bid)])
        self.ask_depth = sum([b['size'] for b in self.get_asks(self.best_ask)])
		
        print(self.best_bid, self.best_ask, end='\r')

    def on_sequence_gap(self, gap_start, gap_end):
        self.reset_book()

    def add(self, order):
        order = {
            'id': order.get('order_id') or order['id'],
            'side': order['side'],
            'price': float(order['price']),
            'size': float(order.get('size') or order['remaining_size'])
        }
        if order['side'] == 'buy':
            bids = self.get_bids(order['price'])
            if bids is None:
                bids = [order]
            else:
                bids.append(order)
            self.set_bids(order['price'], bids)
        else:
            asks = self.get_asks(order['price'])
            if asks is None:
                asks = [order]
            else:
                asks.append(order)
            self.set_asks(order['price'], asks)

    def remove(self, order):
        price = float(order['price'])
        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if bids is not None:
                bids = [o for o in bids if o['id'] != order['order_id']]
                if len(bids) > 0:
                    self.set_bids(price, bids)
                else:
                    self.remove_bids(price)
        else:
            asks = self.get_asks(price)
            if asks is not None:
                asks = [o for o in asks if o['id'] != order['order_id']]
                if len(asks) > 0:
                    self.set_asks(price, asks)
                else:
                    self.remove_asks(price)

    def match(self, order):
        size = float(order['size'])
        price = float(order['price'])

        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if not bids:
                return
            assert bids[0]['id'] == order['maker_order_id']
            if bids[0]['size'] == size:
                self.set_bids(price, bids[1:])
            else:
                bids[0]['size'] -= size
                self.set_bids(price, bids)
            
        else:
            asks = self.get_asks(price)
            if not asks:
                return
            assert asks[0]['id'] == order['maker_order_id']
            if asks[0]['size'] == size:
                self.set_asks(price, asks[1:])
            else:
                asks[0]['size'] -= size
                self.set_asks(price, asks)
            
        self.log_vol([order['time'], order['side'], size])
        

    def change(self, order):
        try:
            new_size = float(order['new_size'])
        except KeyError:
            return

        try:
            price = float(order['price'])
        except KeyError:
            return

        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if bids is None or not any(o['id'] == order['order_id'] for o in bids):
                return
            index = [b['id'] for b in bids].index(order['order_id'])
            bids[index]['size'] = new_size
            self.set_bids(price, bids)
        else:
            asks = self.get_asks(price)
            if asks is None or not any(o['id'] == order['order_id'] for o in asks):
                return
            index = [a['id'] for a in asks].index(order['order_id'])
            asks[index]['size'] = new_size
            self.set_asks(price, asks)

        tree = self._asks if order['side'] == 'sell' else self._bids
        node = tree.get(price)

        if node is None or not any(o['id'] == order['order_id'] for o in node):
            return

    def get_current_ticker(self):
        return self._current_ticker

    def get_current_book(self):
        result = {
            'sequence': self._sequence,
            'asks': [],
            'bids': [],
        }
        for ask in self._asks:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_ask = self._asks[ask]
            except KeyError:
                continue
            for order in this_ask:
                result['asks'].append([order['price'], order['size'], order['id']])
        for bid in self._bids:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_bid = self._bids[bid]
            except KeyError:
                continue

            for order in this_bid:
                result['bids'].append([order['price'], order['size'], order['id']])
        return result

    def get_ask(self):
        return self._asks.min_key()

    def get_asks(self, price):
        return self._asks.get(price)

    def remove_asks(self, price):
        self._asks.remove(price)

    def set_asks(self, price, asks):
        self._asks.insert(price, asks)

    def get_bid(self):
        return self._bids.max_key()

    def get_bids(self, price):
        return self._bids.get(price)

    def remove_bids(self, price):
        self._bids.remove(price)

    def set_bids(self, price, bids):
        self._bids.insert(price, bids)


class MM_Algo(CoinbaseExchangeAuth, OrderBook):

    def __init__(self, product_id='', live=False, nh=False):
        OrderBook.__init__(self, product_id=product_id)
        CoinbaseExchangeAuth.__init__(self)
        if live:
            self.rest_url = "https://api.pro.coinbase.com"
            self.wss_url  = "wss://ws-feed.pro.coinbase.com"
        else:
            self.rest_url = "https://api-public.sandbox.gdax.com"
            self.wss_url  = "wss://ws-feed-public.sandbox.gdax.com"

        self.dtees = 0
        
        self.market       = product_id
        self.order_window = 30
        self.MO_ratio     = 0
        self.connected    = False
        self.got_level2   = 0
        self.orders       = []
        self.o_ids        = {}
        self.ratio        = 0

        self.account_balance = 0
        self.BTC          = 0
        self.tick_size    = 0.01
        self.min_vol      = 0.0001
        self.undercut     = 0
        self.scnd_bid     = 0
        self.scnd_ask     = 0
        self.bid_mark     = 0
        self.ask_mark     = 0
        self.spread       = 0
        self.mid_market   = 0
        self.entry_price  = 0
        self.order_id     = ""
        self.in_trade     = False
        self.order_filled = False

        self.buy_ord_id   = ""
        self.sell_ord_id  = ""

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(("127.0.0.1", 4197))
        self.msg_seq     = 1
        self.target_comp = "Coinbase"
        self.buy_uuid    = ""
        self.sell_uuid   = ""
        self.fix_header  = simplefix.FixMessage()
        self.fix_header.append_pair(8, "FIX.4.2")
        self.fix_header.append_pair(49, self.api_key, header=True)
        self.fix_header.append_pair(56, self.target_comp, header=True)
        
        self.base_fix_buy = simplefix.FixMessage()
        self.base_fix_buy.append_pair(8, "FIX.4.2")
        self.base_fix_buy.append_pair(49, self.api_key, header=True)
        self.base_fix_buy.append_pair(56, self.target_comp, header=True)
        self.base_fix_buy.append_pair(35, "D", header=True)
        self.base_fix_buy.append_pair(21, "1")
        self.base_fix_buy.append_pair(55, self.product_id)
        self.base_fix_buy.append_pair(54, "1") # 1 for buy   order
        self.base_fix_buy.append_pair(40, "2") # 2 for limit order
        self.base_fix_buy.append_pair(59, "P") # post only

        self.base_fix_sell = simplefix.FixMessage()
        self.base_fix_sell.append_pair(8, "FIX.4.2")
        self.base_fix_sell.append_pair(49, self.api_key, header=True)
        self.base_fix_sell.append_pair(56, self.target_comp, header=True)
        self.base_fix_sell.append_pair(35, "D", header=True)
        self.base_fix_sell.append_pair(35, "D", header=True)
        self.base_fix_sell.append_pair(21, "1")
        self.base_fix_sell.append_pair(55, self.product_id)
        self.base_fix_sell.append_pair(54, "2") # 2 for sell  order
        self.base_fix_sell.append_pair(40, "2") # 2 for limit order
        self.base_fix_sell.append_pair(59, "P") # post only

        self.base_fix_cancel = simplefix.FixMessage()
        self.base_fix_cancel.append_pair(8, "FIX.4.2")
        self.base_fix_cancel.append_pair(49, self.api_key, header=True)
        self.base_fix_cancel.append_pair(56, self.target_comp, header=True)
        self.base_fix_cancel.append_pair(35, "F", header=True)
        self.base_fix_cancel.append_pair(55, self.product_id)

        self.buy_msg_rcvd  = False
        self.sell_msg_rcvd = False

        if nh:
            self.update_balance()
            self.FIX_logon()
            self.nhup_threads()
            self.seller()
        
    def update_balance(self):
        try:
            rq = requests.get(self.rest_url + '/accounts', auth=auth).text
        except RequestException:
            return 0

        try:
            #print(rq)
            ac_info = eval(rq.replace('true', 'True').replace('false', 'False'))
            for info in ac_info:
                if info['currency'] == "GBP":
                    self.account_balance = float(info['balance'])
                if info['currency'] == "BTC":
                    self.BTC = float(info['balance'])
        except KeyError:
            return 0
        except SyntaxError:
            return 0

    def on_the_books(self):
        
        while True:
            sleep(0.333)
            try:
                rq = requests.get(self.rest_url + '/accounts', auth=auth).text
                ac_info = eval(rrq.replace('true', 'True').replace('false', 'False'))
            except Exception:
                continue
            
            if type(ac_info) != type([]):
                continue

            for info in ac_info:
                if info['currency'] == "BTC":
                    self.BTC = max(float(info['balance']), float(info['available']), float(info['hold']))

                if info['currency'] == "GBP":
                    self.account_balance = max(float(info['balance']), float(info['available']), float(info['hold']))
                    
    def buy_vol(self):
        fiat_equiv = self.BTC / self.best_ask
        if self.account_balance > fiat_equiv:
            return round(((self.account_balance + fiat_equiv) / 2) / self.best_bid, 8)
        else:
            return self.account_balance / self.best_bid
            
    def sell_vol(self):
        BTC_equiv = self.account_balance / self.best_bid
        if self.BTC > BTC_equiv:
            return self.BTC + BTC_equiv / 2
        else:
            return self.BTC
            
    def buyer(self):
        entry_price = 0
        undercut    = self.tick_size #self.spread * 0.05
        
        while not self.best_bid or not self.best_ask:
            continue
        
        while True:
            order_vol = self.buy_vol()
            #try:
            #    vol_ratio  = (self.bid_depth / self.ask_depth) * self.got_level2
            #except ZeroDivisionError:
            #    print('zero div error')
            #    vol_ratio = 0
            #    self.reset_book()
            
            if order_vol >= self.min_vol: # and self.spread >= self.best_bid * 0.0005:# and vol_ratio >= 1:
                
                if entry_price != self.best_bid:
                    if self.spread >= 0.02:
                        entry_price = self.best_bid + undercut
                    else:
                        entry_price = self.best_bid
                    
                    if self.buy_ord_id != "":
                        self.buy_msg_rcvd = False
                        self.FIX_cancel(self.buy_ord_id, self.buy_uuid, Side="buy")
                        while not self.buy_msg_rcvd:
                            continue
                    
                    self.buy_msg_rcvd = False
                    self.FIX_buy(price=entry_price, vol=order_vol)
                    while not self.buy_msg_rcvd:
                        continue

            #elif order_vol >= self.min_vol and self.buy_ord_id != "":
            #    self.buy_msg_rcvd = False
            #    self.FIX_cancel(self.buy_ord_id, self.buy_uuid, Side="buy")
            #    while not self.buy_msg_rcvd:
            #        continue

    def seller(self):
        entry_price = 10000000000
        
        while not self.best_bid:
            continue
        
        while True:
            
            order_vol = self.sell_vol()
            
            if order_vol >= self.min_vol:
                
                if entry_price != self.best_ask:
                    if self.spread >= 0.02:
                        entry_price = self.best_ask - self.tick_size
                    else:
                        entry_price = self.best_ask
                    
                    if self.sell_ord_id != "":
                        self.sell_msg_rcvd = False
                        self.FIX_cancel(self.sell_ord_id, self.sell_uuid, Side="sell")
                        while not self.sell_msg_rcvd:
                            continue

                    self.sell_msg_rcvd = False
                    self.FIX_sell(price=entry_price, vol=order_vol)
                    while not self.sell_msg_rcvd:
                        continue
                        
                #elif order_vol >= self.min_vol and self.sell_ord_id != "":
                #   self.sell_msg_rcvd = False
                #   self.FIX_cancel(self.sell_ord_id, self.sell_uuid, Side="sell")
                #   while not self.sell_msg_rcvd:
                #       continue
            
    #------------------------------FIX--------------------------------------#
    def FIX_logon(self):
        t         = str(datetime.utcnow()).replace("-","").replace(" ", "-")[:-3]
        message   = [t, "A", "1", self.api_key, self.target_comp, self.api_pwd]
        message   = chr(1).join(message).encode('ascii')

        hmac_key  = b64decode(self.api_sec)
        signature = new(hmac_key, message, sha256)
        sign_b64  = b64encode(signature.digest()).decode()

        self.msg_seq = 1
        logon_msg = simplefix.FixMessage()
        logon_msg.append_pair(8, "FIX.4.2")
        logon_msg.append_pair(49, self.api_key, header=True)
        logon_msg.append_pair(56, self.target_comp, header=True)
        logon_msg.append_pair(35, "A")
        logon_msg.append_pair(34, str(self.msg_seq), header=True)
        logon_msg.append_pair(52, t, header=True)
        logon_msg.append_pair(98, "0")
        logon_msg.append_pair(108, "30")
        logon_msg.append_pair(554, self.api_pwd)
        logon_msg.append_pair(96, sign_b64)
        
        logon = logon_msg.encode()

        self.s.sendall(logon)
        r  = ""
        while "\x0110=" not in r:
            try:
                r += self.s.recv(5).decode()
            except ConnectionResetError:
                break
        r = r[r.find('8=FIX.4.2'):]

        start   = r.find('35=')
        end     = r.find('\x01', start)
        MsgType = r[start+3:end]
        if MsgType == "A":
            print('FIX logon: SUCCESS')
        else:
            print('FIX logon: FAILED')
            print('Server response:', r, '\n')

        self.msg_seq += 1

    def FIX_logout(self):
        logout_msg = simplefix.FixMessage()

        logout_msg.append_pair(8, "FIX.4.2")
        logout_msg.append_pair(35, "5")
        logout_msg.append_pair(49, self.api_key, header=True)
        logout_msg.append_pair(56, self.target_comp, header=True)
        logout_msg.append_pair(34, self.msg_seq, header=True)
        logout_msg.append_pair(52, str(datetime.utcnow()).replace("-","").replace(" ", "-")[:-3], header=True)

        logout = logout_msg.encode()

        self.s.sendall(logout)
        r  = ""
        while "\x0110=" not in r:
            try:
                r += self.s.recv(5).decode()
            except ConnectionResetError:
                continue
        r = r[r.find('8=FIX.4.2'):]

        start   = r.find('35=')
        end     = r.find('\x01', start)
        MsgType = r[start+3:end]
        if MsgType == "5":
            print('FIX logout: SUCCESS')
        else:
            print('FIX logout: FAILED')
            print('Server response:', r, '\n')

        self.msg_seq = 1

    def FIX_buy(self, price, vol):
        global lock
        
        buy_msg = self.base_fix_buy
        buy_msg.append_pair(11, uuid4())
        buy_msg.append_pair(44, str(round(price, 2)))
        buy_msg.append_pair(38, str(vol))

        with lock:
            buy_msg.append_pair(34, str(self.msg_seq), header=True)
            order = buy_msg.encode()
            try:
                self.s.sendall(order)
            except ConnectionResetError:
                self.buy_msg_rcvd = True

            self.msg_seq += 1

    def FIX_sell(self, price, vol):
        global lock

        sell_msg = self.base_fix_sell
        sell_msg.append_pair(11, uuid4())
        sell_msg.append_pair(44, str(round(price, 2)))
        sell_msg.append_pair(38, str(vol))

        with lock:
            sell_msg.append_pair(34, str(self.msg_seq), header=True)
            order = sell_msg.encode()
            try:
                self.s.sendall(order)
            except ConnectionResetError:
                self.sell_msg_rcvd = True
            
            self.msg_seq += 1

    def FIX_cancel(self, OrderId, OrigClOrdID, Side):
        global lock
        
        cancel_msg = self.base_fix_cancel
        cancel_msg.append_pair(11, uuid4())
        cancel_msg.append_pair(37, OrderId)
        cancel_msg.append_pair(41, OrigClOrdID)

        with lock:
            cancel_msg.append_pair(34, str(self.msg_seq))
            order = cancel_msg.encode()
            r = ""
            try:
                self.s.sendall(order)
            except ConnectionResetError:
                if Side == "buy":
                    self.buy_msg_rcvd  = True
                elif Side == "sell":
                    self.sell_msg_rcvd = True
            
            self.msg_seq += 1

    def FIX_heartbeat(self):
        global lock
        heartbeat = simplefix.FixMessage()
        heartbeat.append_pair(8, "FIX.4.2")
        heartbeat.append_pair(49, self.api_key, header=True)
        heartbeat.append_pair(56, self.target_comp, header=True)
        heartbeat.append_pair(35, "0")
        
        while True:
            if int(clock()) % 30 == 0:
                hb = heartbeat
                with lock:
                    hb.append_pair(34, str(self.msg_seq))
                    hb = heartbeat.encode()
                    try:
                        self.s.sendall(hb)
                    except ConnectionResetError:
                        continue
                    self.msg_seq += 1
                    sleep(1)

    def FIX_listener(self):
        global lock

        while True:
            t0 = clock()
            r = ""
            while "\x0110=" not in r[-10:]:
                try:
                    r += self.s.recv(10).decode()
                except ConnectionResetError:
                    print('FIX conn error')
                    with lock:
                        self.FIX_logout()
                        self.FIX_logon() 
            #r = r[r.find('8=FIX.4.2'):]
            
            #dt = datetime.utcnow() - datetime.strptime(self.extract_field(r, '60='), '%Y%m%d-%H:%M:%S.%f')
            print(r)
            MsgType  = self.extract_field(r, '35=')
            if MsgType == "0" or MsgType == "1":
                continue
            Side     = self.extract_field(r, '54=')
            ExecType = self.extract_field(r, '150=')
            #print(len(r), MsgType, ExecType, clock()-t0)
            
            if MsgType == "8":
                if ExecType == "1" or ExecType == "0":
                    if Side == "1":
                        self.buy_ord_id   = self.extract_field(r, '37=')
                        self.buy_uuid     = self.extract_field(r, '11=')
                        self.buy_msg_rcvd = True
                    elif Side == "2":
                        self.sell_ord_id   = self.extract_field(r, '37=')
                        self.sell_uuid     = self.extract_field(r, '11=')
                        self.sell_msg_rcvd = True
                elif ExecType == "4" or ExecType == "8":
                    if Side == "1":
                        self.buy_ord_id   = ""
                        self.buy_msg_rcvd = True
                    elif Side == "2":
                        self.sell_ord_id   = ""
                        self.sell_msg_rcvd = True
            elif MsgType == "9":
                OrderId = self.extract_field(r, '37=')
                if OrderId == self.buy_ord_id:
                    self.buy_msg_rcvd = True
                elif OrderId == self.sell_ord_id:
                    self.sell_msg_rcvd = True
            elif MsgType == "3":
                self.buy_msg_rcvd  = True
                self.sell_msg_rcvd = True

    def extract_field(self, msg, field):
        start = msg.find(field)
        end   = msg.find('\x01', start)
        return msg[start+len(field):end]
    
    #-----------------------------------------------------------------------#
    
    def run_threads(self):
        threads = []
        
        def _full_channel():
            self.full_channel(self)

        threads.append(Thread(target=_full_channel, daemon=True))
        threads.append(Thread(target=self.on_the_books, daemon=True))
        threads.append(Thread(target=self.FIX_listener, daemon=True))
        
        #threads.append(Thread(target=self.buyer, daemon=True))
        #threads.append(Thread(target=self.seller, daemon=True))
        
        for t in threads:
            t.start()
                

if __name__ == "__main__":
    auth = CoinbaseExchangeAuth()
    algo = MM_Algo(product_id='BTC-GBP', live=True, nh=nhup)
    algo.update_balance()
    print('Coinbase Pro - Market Maker')
    algo.FIX_logon()
    print('Account balance:', round(algo.account_balance, 2))
    algo.run_threads()
    command_line(algo)
