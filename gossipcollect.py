import codecs, grpc, os, etcd3, time, json
import lightning_pb2 as lnrpc, lightning_pb2_grpc as lightningstub

etcd = etcd3.client()

def setup_lnd_rpc():
    LND_DIR="/mnt/hdd/lnd"
    macaroon = codecs.encode(open(f'{LND_DIR}/data/chain/bitcoin/mainnet/admin.macaroon', 'rb').read(), 'hex')
    os.environ['GRPC_SSL_CIPHER_SUITES'] = 'HIGH+ECDSA'
    cert = open(f'{LND_DIR}/tls.cert', 'rb').read()
    ssl_creds = grpc.ssl_channel_credentials(cert)
    channel = grpc.secure_channel('localhost:10009', ssl_creds)
    stub = lightningstub.LightningStub(channel)
    request = lnrpc.GraphTopologySubscription()
    return stub, request, macaroon

class RoutingPolicy:
    def __init__(self, routing_policy):
        self.tld = routing_policy.time_lock_delta
        self.min_htlc = routing_policy.min_htlc
        self.fee_base_msat = routing_policy.fee_base_msat
        self.fee_rate_milli_msat = routing_policy.fee_rate_milli_msat
        self.disabled = routing_policy.disabled if routing_policy.disabled else None
        self.max_htlc_msat = routing_policy.max_htlc_msat
        self.last_update = routing_policy.last_update if routing_policy.last_update else None

    def toJSON(self):
        ret = dict()
        ret["tld"] = self.tld
        ret["min_htlc"] = self.min_htlc
        ret["fee_base_msat"] = self.fee_base_msat
        ret["fee_rate_milli_msat"] = self.fee_rate_milli_msat
        ret["disabled"] = self.disabled
        ret["max_htlc_msat"] = self.max_htlc_msat
        ret["last_update"] = self.last_update
        return ret

    def __str__(self):
        return str(self.toJSON())

class NodeAddress:
    def __init__(self, node_address):
        self.network = node_address.network
        self.addr = node_address.addr

    def toJSON(self):
        ret = dict()
        ret["network"] = self.network
        ret["addr"] = self.addr
        return ret

    def __str__(self):
        return str(self.toJSON())

class NodeUpdate:
    def __init__(self, node_update):
        self.identity_key = node_update.identity_key
        self.alias = node_update.alias
        self.color = node_update.color
        self.node_addresses = [NodeAddress(addr) for addr in node_update.node_addresses]

    def toJSON(self):
        ret = dict()
        ret["alias"] = self.alias
        ret["color"] = self.color
        ret["node_addresses"] = [str(addr) for addr in self.node_addresses]
        return ret

    def __str__(self):
        return str(self.toJSON())

class ChannelEdgeUpdate:
    def __init__(self, chan_update):
        self.chan_id = chan_update.chan_id
        self.capacity = chan_update.capacity
        self.routing_policy = RoutingPolicy(chan_update.routing_policy)
        self.advertising_node = chan_update.advertising_node
        self.connecting_node = chan_update.connecting_node

    def toJSON(self):
        ret = dict()
        ret["chan_id"] = self.chan_id
        ret["capacity"] = self.capacity
        ret["routing_policy"] = self.routing_policy.toJSON() 
        #ret["advertising_node"] = self.advertising_node
        #ret["connecting_node"] = self.connecting_node
        return ret

    def __str__(self):
        return str(self.toJSON())

class ClosedChannelUpdate:
    def __init__(self, closed_chan):
        self.chan_id = closed_chan.chan_id
        self.capacity = closed_chan.capacity
        self.closed_height = closed_chan.closed_height

    def toJSON(self):
        ret = dict()
        ret["capacity"] = self.capacity
        ret["closed_height"] = self.closed_height
        return ret

    def __str__(self):
        return str(self.toJSON())


def get_time():
    #unix milliseconds
    return int( time.time_ns() / 1000 )

def manage_node_update(node_update):
    node_upd = NodeUpdate(node_update)
    key = f"node_update/{node_upd.identity_key}/{get_time()}"
    etcd.put(key, json.dumps(node_upd.toJSON()))
    print(f"PUT {key} successful")

def manage_chan_update(chan_update):
    chan_upd = ChannelEdgeUpdate(chan_update)
    key = f"channel_update/{chan_upd.advertising_node}/{get_time()}"
    etcd.put(key, json.dumps(chan_upd.toJSON()))
    #result = etcd.get(key)
    #print(json.loads(result[0]))
    print(f"PUT {key} successful")

def manage_closed_chan(closed_chan):
    closed_chan_upd = ClosedChannelUpdate(closed_chan)
    key = f"closed_channel/{closed_chan_upd.chan_id}/{get_time()}"
    etcd.put(key, json.dumps(closed_chan_upd.toJSON()))
    print(f"PUT {key} successful")

def main():
    stub, req, macaroon = setup_lnd_rpc()
    for response in stub.SubscribeChannelGraph(req, metadata=[('macaroon', macaroon)]):
        for node_update in response.node_updates:
            manage_node_update(node_update)
        for chan_update in response.channel_updates:
            manage_chan_update(chan_update)
        for closed_chan in response.closed_chans:
            manage_closed_chan(closed_chan)

if __name__ == '__main__':
    main()

