#!/usr/bin/python3
import asyncio
import random
import traceback
import aiomas
import hashlib
import logging
import errno

from helpers.storage import Storage

CHORD_FINGER_TABLE_SIZE = 8 # TODO: 256
CHORD_RING_SIZE = 2**CHORD_FINGER_TABLE_SIZE  # Maximum number of addresses in the Chord network

def in_interval(search_id, node_left, node_right, inclusive_left=False, inclusive_right=False):
    """
    Interval checks.
    """
    # Special case must not have any manipulation to
    if node_left != node_right:
        if inclusive_left:
            node_left = (node_left - 1) % CHORD_RING_SIZE
        if inclusive_right:
            node_right = (node_right + 1) % CHORD_RING_SIZE

    if node_left < node_right:
        return node_left < search_id < node_right
    else:
        # First eq: search area covered is before 0
        # Second eq: search area covered is after 0
        #
        # Special case: node_left == node_right
        #   This interval is assumed to contain every ID. This is needed if the current network
        #   only consists of the bootstrap node.
        #   Example: random_ID is in (249,249]
        return (search_id > max(node_left, node_right)) or \
               (search_id < min(node_left, node_right))


# def deserialize_minimal_node(obj):
#     predecessor = None
#     if "predecessor" in obj:
#         predecessor = Node(None, None)
#         predecessor.setup_node(node_id=obj["predecessor"]["node_id"], node_address=obj["predecessor"]["node_address"])
#
#     node = Node(None, None)
#     node.setup_node(node_id=obj["node_id"], node_address=obj["node_address"], predecessor=predecessor)
#
#     return node


# class Finger(object):
#     def __init__(self, start_id, successor=None):
#         self.startID = start_id
#         self.successor = successor
#
#     def __repr__(self):
#         return 'Start: ' + str(self.startID) + ', SuccessorID: ' + str(self.successor.nodeId)


class Node(aiomas.Agent):

    def __init__(self, container, node_address):
        # Async RPC init
        super().__init__(container, node_address)
        self.node_address = node_address
        # Logging
        self.log = logging.getLogger(__name__)
        self.log.info("Node server listening on %s.", node_address)
        # Node state
        self.activated = True
        self.network_timeout = 10
        self.fix_interval = 7 + random.randint(0, 10)
        self.storage = Storage()
        # Overlay network info
        self.fingertable = []

    def as_dict(self, serialize_neighbors=False):
        dict_node = {
            "node_id": self.id,
            "node_address": self.node_address,
        }
        if serialize_neighbors:
            dict_node["successor"] = self.fingertable[0]["successor"]
        if serialize_neighbors and self.predecessor:
            dict_node["predecessor"] = self.predecessor

        return dict_node

    @staticmethod
    def generate_key(address):
        # TODO: public key hash instead of protocol + IP address + port
        # TODO: remove modulo
        return int(hashlib.sha256(address.encode()).hexdigest(), 16) % CHORD_RING_SIZE

    # def get_entry(self, position):
    #     # Todo: rethink whether this makes sense
    #     addition = 2**(position-1)
    #     entry = (self.id + addition) % CHORD_RING_SIZE
    #     return entry

    @aiomas.expose
    def get_node_id(self):
        return self.id

    @asyncio.coroutine
    def join(self, node_id=None, node_address=None, bootstrap_address=None):
        """
        Set ups all internal state variables needed for operation.
        Needs to be called previously to any other function or RPC call.

        :param node_id:
            optional node ID.
            If not supplied, it will be generated automatically.

        :param node_address:
            optional node address formatted as an aiomas agent address (IPv4 or IPv6 address)

        :param bootstrap_address:
            if not given, a new Chord network is created. Otherwise, the new node
            will gather the required information to integrate into the Chord network.
        """
        self.id = node_id or self.generate_key(self.node_address)
        self.node_address = self.node_address or node_address   # normally already set in __init__
        self.bootstrap_address = bootstrap_address
        self.predecessor = None
        self.log.info("[Configuration]  node_id: %d, bootstrap_node: %s", self.id, self.bootstrap_address)

        if self.bootstrap_address:
            # Regular node joining via bootstrap node
            yield from self.init_finger_table()
            yield from self.update_others()
        else:
            # This is the bootstrap node
            successor_node = self.as_dict()
            self.__generate_fingers(successor_node)
            # self.predecessor = successor_node  # If removed, easier to replace with checks on update_predecessor
        self.print_finger_table()

        if self.bootstrap_address:
            remote_peer = yield from self.container.connect(self.bootstrap_address)
            ft =  yield from remote_peer.rpc_get_fingertable()
            print("Bootstrap Finger Table: ")
            self.print_finger_table(ft)

    @asyncio.coroutine
    def update_others(self):
        """
        Update peers' finger table that should refer to our node and notify them.
        """
        for k in range(0, CHORD_FINGER_TABLE_SIZE):
            p = yield from self.find_predecessor((self.id - 2**k) % CHORD_RING_SIZE)
            self.log.info("Update peer: %s", p)
            if self.id != p["node_id"]:
                remote_peer = yield from self.container.connect(p["node_address"])
                yield from remote_peer.rpc_update_finger_table(self.as_dict(), k)

    @asyncio.coroutine
    def init_finger_table(self):
        print("in init finger")
        self.__generate_fingers(None)

        remote_peer = yield from self.container.connect(self.bootstrap_address)
        # print("Looking for %s" % self.fingertable[0]["start"])
        successor = yield from remote_peer.rpc_find_successor(self.fingertable[0]["start"])
        self.fingertable[0]["successor"] = successor  # TODO: validate successor
        self.print_finger_table()

        # Fix references of our direct neighbors
        yield from self.update_neighbors(successor)

        # Retrieve successor node for each finger 0 -> m-1 (finger 0 is already retrieved from bootstrap node)
        for k in range(CHORD_FINGER_TABLE_SIZE - 1):
            finger = self.fingertable[k]
            finger_next = self.fingertable[k + 1]

            if in_interval(finger_next["start"], self.id, finger["successor"]["node_id"], inclusive_left=True):
                self.log.info("Copy previous finger: %d in between [%d, %d)",
                              finger_next["start"],
                              self.id,
                              finger["successor"]["node_id"])
                # Reuse previous finger
                finger_next["successor"] = finger["successor"]
            else:
                self.log.info("Exceeding our successor, need a RPC.")
                # TODO: validate data
                # BUG: if only 2 nodes in network, the node being responsible for the requested start ID
                #      is wrong because bootstrap node does not updated its table yet
                finger_successor = yield from remote_peer.rpc_find_successor(finger_next["start"])
                self.log.info("Node for %d: %s", finger_next["start"], finger_successor)
                finger_next["successor"] = finger_successor


        # Optimization for joining node (if not bootstrap node)
        # - Find close node to myself (e.g., successor)
        # - Request finger table and store temporary_entries
        # - for each of my needed finger table starts, use closest entries and directly ask this node.
        # - Fallback to node asked previously (or bootstrap node as last fallback) if node is not responding

    def __generate_fingers(self, successor_reference):
        self.fingertable = []

        for k in range(0, CHORD_FINGER_TABLE_SIZE):
            entry = {
                "start": ((self.id + 2**k) % CHORD_RING_SIZE),
                "successor": successor_reference
            }
            # TODO: add successor if not bootstrap node
            self.fingertable.append(entry)

        self.log.info("Default finger table: %s", str(self.fingertable)+"\n\n")

    def print_finger_table(self, fingerTableToPrint=None):
        if not fingerTableToPrint:
            fingerTableToPrint = self.fingertable

        print(" START  |   ID ")
        print("-----------------------")
        for tableEntry in fingerTableToPrint:
            if tableEntry["successor"]:
                print("%s  %s" % (str(tableEntry["start"]).ljust(4), tableEntry["successor"]["node_id"]))
            else:
                print(str(tableEntry["start"]).ljust(4)+ "  -  ")


        if self.predecessor:
            print("Predecessor ID: %d" % self.predecessor["node_id"])

    @asyncio.coroutine
    def update_finger_table(self, origin_node, i):
        if in_interval(origin_node["node_id"], self.id, self.fingertable[i]["successor"]["node_id"], inclusive_left=True):
            self.log.info("For finger %d: origin_node is %s; successor was %s",
                          i, origin_node, self.fingertable[i]["successor"]["node_id"])

            self.fingertable[i]["successor"] = origin_node
            # Bug: this recursive call causes wrong finger tables. The original paper seems to be incorrect here.
            # remote_peer = yield from self.container.connect(self.predecessor["node_address"])
            # yield from remote_peer.rpc_update_finger_table(origin_node, i)

    @asyncio.coroutine
    def update_neighbors(self, successor):
        """
        Update our successor's pointer to reference us as immediate predecessor
        (according to default Chord specification).
        Notify our direct predecessor about our presence. This allows to early stabilize
        its immediate successor finger[0] early.

        :param successor:
            the immediate successor of this node (e.g., according to a bootstrap node).
        """
        # Fix predecessor reference on our immediate successor
        remote_peer = yield from self.container.connect(successor["node_address"])
        update_pred = yield from remote_peer.rpc_update_predecessor(self.as_dict())
        self.log.debug("Predecessor update result: %s", update_pred)
        # TODO: validate input
        if "old_predecessor" in update_pred:
            # Use successor node if chord overlay only has bootstrap node as only one
            self.predecessor = update_pred["old_predecessor"]
            self.log.info("Set predecessor: %s", self.predecessor)

            # Notify our predecessor to be aware of us (new immediate successor)
            remote_peer = yield from self.container.connect(self.predecessor["node_address"])
            yield from remote_peer.rpc_update_successor(self.as_dict())
        else:
            # Something went wrong during update
            # A new node might have joined in the meantime -> TODO: update our reference or clean exit
            self.log.error("Could not update predecessor reference of our successor. Try restarting.")

    @asyncio.coroutine
    def update_successor(self, new_node=None):
        """
        Updates the reference to our immediate successor.

        :param new_node:
            should be None only for periodic fix ups.
            In this case, the predecessor of our old successor is accepted as our new successor.
        """
        old_successor = self.fingertable[0]["successor"]
        # Check: DoS possible here?
        # TODO: validation + timeout catch
        old_successor_view, peer_err = yield from self.run_rpc_safe(old_successor["node_address"],
                                                                    "rpc_get_node_info")
        if peer_err != 0:
            # Immediate successor is not responding
            self.log.warn("Immediate successor %s not responding.", old_successor)
            return  # TODO: better error handling

        # For periodic checks assume that our current immediate successor knows better
        periodic_fix = False
        if new_node is None:
            new_node = old_successor_view["predecessor"]
            periodic_fix = True
            # print("new %d between (%d,%d)" % (new_node["node_id"], self.id, old_successor["node_id"]))

        # New successor before old one or old one not responding anymore (last option is TODO)
        if in_interval(new_node["node_id"], self.id, old_successor["node_id"]):
            # Check old successor whether it already accepted new node

            if old_successor_view["predecessor"]["node_address"] == new_node["node_address"]:
                # Update finger table to point to new immediate successor
                self.fingertable[0]["successor"] = new_node
                self.log.info("Updated successor reference to node %d (%s)",
                              new_node["node_id"], new_node["node_address"])

                # Assure that the new immediate successor links to us
                if periodic_fix:
                    successor_peer = yield from self.container.connect(self.fingertable[0]["successor"]["node_address"])
                    update_pred = yield from successor_peer.rpc_update_predecessor(self.as_dict())
                    self.log.info("Updated successor's predecessor link: %s", update_pred)

            else:
                # Do not update, only mention suspicious observation
                self.log.error("Node %d (%s) wants to be our immediate successor, but original successor %d (%s) "
                               "does not reference it. Looks malicious.",
                               new_node["node_id"], new_node["node_address"],
                               old_successor["node_id"], old_successor["node_address"])

    @asyncio.coroutine
    def fix_finger(self, finger_id=-1):
        """
        Resolves the responsible node for the given finger and updates it accordingly.

        :param finger_id:
            index of the finger table to update.
            The value should be between 0 and length of the finger table.
        """
        if not (0 <= finger_id < len(self.fingertable)):
            raise IndexError("No valid finger ID.")

        cur_finger = self.fingertable[finger_id]
        successor = yield from self.find_successor(cur_finger["start"])
        print("For start %d, successor is '%s'" % (cur_finger["start"], successor))

        if successor != cur_finger["successor"]:
            self.log.info("Finger %d updated: successor is now %s (old: %s)",
                          finger_id, successor, cur_finger["successor"])
            cur_finger["successor"] = successor
        # else:
        #     self.log.warn("Received successor for finger %d not fitting to ID ranges in finger table: %d not in [%d, %d)",
        #                   finger_id, successor["node_id"], cur_finger["start"], next_finger["start"])

    @asyncio.coroutine
    def find_successor(self, node_id):
        successor = self.fingertable[0]["successor"]
        if in_interval(node_id, self.id, successor["node_id"], inclusive_right=True):
            return successor
        else:
            node = yield from self.find_predecessor(node_id)
            print("[find_successor] Calculated node for %d: %s" % (node_id, node))
            return node["successor"]  # Attention: relies on available successor information which has to be
                                      # retrieved by closest_preceding_finger()

    @asyncio.coroutine
    def find_predecessor(self, node_id):
        selected_node = self.as_dict(serialize_neighbors=True)
        previous_selected_node = None

        while not in_interval(node_id, selected_node["node_id"], selected_node["successor"]["node_id"], inclusive_right=True):
            self.log.info("Node ID %d not in interval (%d, %d]",
                          node_id,
                          selected_node["node_id"],
                          selected_node["successor"]["node_id"])
            if selected_node["node_id"] == self.id:
                # Typically in first round: use our finger table to locate close peer
                print("Looking for predecessor of %d in first round." % node_id)
                selected_node = yield from self.get_closest_preceding_finger(node_id)

                print("Closest finger: %s" % selected_node)
                # If still our self, we do not know closer peer and should stop searching
                # if selected_node["node_id"] == self.id:
                #     break

            else:
                # For all other remote peers, we have to do a RPC here
                self.log.debug("Starting remote call.")
                peer = yield from self.container.connect(selected_node["node_address"])
                selected_node = yield from peer.rpc_get_closest_preceding_finger(node_id)
                # TODO: validate received input before continuing the loop
                self.log.info("Remote closest node for ID %d: %s", node_id, str(selected_node))

            # Detect loop without progress
            if previous_selected_node == selected_node:
                self.log.error("No progress while looking for node closer to ID %d than node %s", node_id, selected_node)
                raise aiomas.RemoteException("Remote peer did not return more closer node to given Id " + str(node_id), "")
            previous_selected_node = selected_node

        return selected_node

    @asyncio.coroutine
    def get_closest_preceding_finger(self, node_id):
        """
        Find closest preceding finger within m -> 0 fingers.
        This method tries falling back to a less optimal peer if the intended peer from
        the finger table does not respond within ``network_timeout`.

        As the finger table only contains elementary information about a possible successor peer,
        the resulting peer is requested for more details (e.g., its successor and predecessor).

        :param node_id:
            node ID as an integer.

        :return:
            returns the interesting node descriptor as a dictionary with successor and predecessor.
        :rtype: dict
        """
        for k in range(CHORD_FINGER_TABLE_SIZE - 1, -1, -1):
            finger_successor = self.fingertable[k]["successor"]
            self.log.debug("Iterate finger %d: %d in %s", k, node_id, self.fingertable[k])

            if in_interval(finger_successor["node_id"], self.id, node_id):
                # Augment node with infos about its successor (and its predecessor)
                # TODO: validation
                finger_successor, status = yield from self.run_rpc_safe(finger_successor["node_address"],
                                                                        "rpc_get_node_info")
                return finger_successor

        return self.as_dict(serialize_neighbors=True)

    @asyncio.coroutine
    def stabilize(self):
        while self.activated:
            yield from asyncio.sleep(self.fix_interval)
            self.log.info("Running periodic fix up.")

            # Assure that successor still references us as immediate predecessor
            yield from self.update_successor()
            # Do random updates on the finger table
            finger = random.randint(0, CHORD_FINGER_TABLE_SIZE - 1)  # better to use pseudo rnd: every finger once first
            yield from self.fix_finger(finger)

            print("Current finger table:")
            self.print_finger_table()
            print("Stored entries: ", len(self.storage.data))

    @asyncio.coroutine
    def put_data(self, key, data, ttl):
        storage_node = yield from self.find_successor(key)
        print("Found successor for storage: ", storage_node)

        if storage_node["node_id"] == self.id:
            self.storage.put(key, data, ttl=ttl)
            return {
                "status": 0
            }
        else:
            # Directly connect to remote peer and store it there
            peer = yield from self.container.connect(storage_node["node_address"])
            result = yield from peer.rpc_dht_put_data(key, data, ttl=ttl)  # TODO: validate
            return result

    @asyncio.coroutine
    def get_data(self, key):
        storage_node = yield from self.find_successor(key)

        if storage_node["node_id"] == self.id:
            return {
                "status": 0,
                "data": self.storage.get(key)
            }
        else:
            # Directly connect to remote peer and fetch data from there
            peer = yield from self.container.connect(storage_node["node_address"])
            result = yield from peer.rpc_dht_get_data(key)  # TODO: validate
            return result

    ##########################################################################
    ### RPC wrappers and functions for maintaining Chord's network overlay ###
    @asyncio.coroutine
    def run_rpc_safe(self, remote_address, func_name, *args):
        data = None
        err = 1
        try:
            #print("Before container.connect()")
            fut_peer = self.container.connect(remote_address)
            remote_peer = yield from asyncio.wait_for(fut_peer, timeout=self.network_timeout)
            #print("After connect()")
            # Invoke remote function
            data = yield from getattr(remote_peer, func_name, *args)()
            err = 0

        except (asyncio.TimeoutError, asyncio.CancelledError) as e:
            err = errno.ETIMEDOUT
            self.log.warn("AsyncIO error: connection timed out to remote peer %s", remote_address)

        except TimeoutError:
            err = errno.ETIMEDOUT
            self.log.warn("Connection timed out to remote peer %s", remote_address)

        except ConnectionRefusedError:
            err = errno.ECONNREFUSED
            self.log.warn("Connection refused by remote peer %s", remote_address)

        except ConnectionError:
            # Base for connection related issues
            err = errno.ECOMM
            self.log.warn("Error connecting to %s", remote_address)

        except Exception:
            err = 1
            self.log.warn("Unhandled error during RPC to %s", remote_address)
            traceback.print_exc()

        return data, err

    @aiomas.expose
    def rpc_get_node_info(self):
        return self.as_dict(serialize_neighbors=True)

    @aiomas.expose
    def rpc_get_fingertable(self):
        # Check: might be critical to be published completely
        return self.fingertable

    @aiomas.expose
    def rpc_update_predecessor(self, remote_node):
        if not isinstance(remote_node, dict):
            raise TypeError('Invalid type in argument.')

        remote_id = remote_node["node_id"]
        remote_addr = remote_node["node_address"]
        # TODO: connect old predecessor if new node ID is not closer to us
        if self.predecessor is None or in_interval(remote_id, self.predecessor["node_id"], self.id):
            # If this is a bootstrap node and this is the first node joining,
            # set predecessor of new node to us. Like this, the ring topology is properly maintained
            old_predecessor = self.predecessor or self.as_dict()
            self.predecessor = {"node_id": remote_id, "node_address": remote_addr}

            res = self.predecessor.copy()
            res["old_predecessor"] = old_predecessor
            return res
        else:
            # No change for this node's predecessor, because it is closer to our node.
            # Probably, some nodes requested on the way to us, do not have a proper view on this overlay network.
            # Note: Might be better to raise exception
            return self.as_dict()

    @aiomas.expose
    def rpc_update_successor(self, node_hint):
        if not isinstance(node_hint, dict):
            raise TypeError('Invalid type in argument.')

        yield from self.update_successor(new_node=node_hint)

    @aiomas.expose
    def rpc_update_finger_table(self, origin_node, i):
        # TODO: Update finger table
        yield from self.update_finger_table(origin_node, i)
        return True

    @aiomas.expose
    def rpc_find_successor(self, node_id):
        # TODO: validate params to prevent attacks!
        res = yield from self.find_successor(node_id)
        return res

    @aiomas.expose
    def rpc_get_closest_preceding_finger(self, node_id):
        # TODO: validate params to prevent attacks!
        res = yield from self.get_closest_preceding_finger(node_id)
        return res

    ### RPC Data storage ###
    @aiomas.expose
    def rpc_dht_put_data(self, key, data, ttl):
        # TODO: validate
        if in_interval(key, self.predecessor["node_id"], self.id, inclusive_right=True):
            self.storage.put(key, data, ttl=ttl)
            return {
                "status": 0
            }
        else:
            self.log.warn("This node %d is not responsible for storing data with key %d.",
                          self.id, key)
            return {
                "status": -1,
                "message": "not responsible"
            }

    @aiomas.expose
    def rpc_dht_get_data(self, key):
        if in_interval(key, self.predecessor["node_id"], self.id, inclusive_right=True):
            data = self.storage.get(key)
            return {
                "status": 0,
                "data": data
            }
        else:
            return {
                "status": -1
            }

    ### RPC tests ###
    @asyncio.coroutine
    def test_get_node_id(self, addr):
        # RPC to remote node
        remote_agent = yield from self.container.connect(addr)
        id = yield from remote_agent.get_node_id()
        print("%s got answer from %s: ID is %d" % (self.node_address, addr, id))

    @asyncio.coroutine
    def test_get_closest_preceding_finger(self, addr, node_id):
        # RPC to remote node
        remote_agent = yield from self.container.connect(addr)
        res_node = yield from remote_agent.rpc_get_closest_preceding_finger(node_id)
        print("%s got answer from %s: closest node is %s" % (self.node_address, addr, str(res_node)))

    @asyncio.coroutine
    def test_find_my_successor(self, addr):
        # RPC to remote node
        remote_agent = yield from self.container.connect(addr)
        res_node = yield from remote_agent.rpc_find_successor(self.id + 1)
        print("%s got answer from %s: my successor is %s" % (self.node_address, addr, str(res_node)))
