#!/usr/bin/python3
import asyncio
import random
import traceback
import aiomas
import hashlib
import logging
import errno

from helpers.storage import Storage
from helpers.replica import Replica
from helpers.messageDefinitions import *
from jsonschema import validate, Draft3Validator
from jsonschema.exceptions import ValidationError, SchemaError
from helpers.validator import *

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


def strip_node_response(data, immediate_neighbors=False, trace_log=False):
    if data is None:
        return None

    output = {
        "node_id": data["node_id"],
        "node_address": data["node_address"]
    }
    if immediate_neighbors and "successor" in data:
        output["successor"] = strip_node_response(data["successor"])
    if immediate_neighbors and "predecessor" in data:
        output["predecessor"] = strip_node_response(data["predecessor"])
    if trace_log:
        output["trace"] = data["trace"]

    return output

# class Finger(object):
#     def __init__(self, start_id, successor=None):
#         self.startID = start_id
#         self.successor = successor
#
#     def __repr__(self):
#         return 'Start: ' + str(self.startID) + ', SuccessorID: ' + str(self.successor.nodeId)


class Node(aiomas.Agent):
    """
    Node
    """

    def __init__(self, container, node_address):
        # Async RPC init
        super().__init__(container, node_address)
        self.node_address = node_address

        self.log = logging.getLogger(__name__)
        self.log.info("Node server listening on %s.", node_address)

        # Node state
        self.activated = True
        self.network_timeout = 10
        self.storage = Storage()
        # Overlay network
        self.fingertable = []
        self.fix_interval = 7 + random.randint(0, 10)
        self.fix_next = 0

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

        # if self.bootstrap_address:
        #     remote_peer = yield from self.container.connect(self.bootstrap_address)
        #     ft =  yield from remote_peer.rpc_get_fingertable()
        #     print("Bootstrap Finger Table: ")
        #     self.print_finger_table(ft)

    @asyncio.coroutine
    def init_finger_table(self):
        """Generates basic finger table for joining nodes.
        """
        self.__generate_fingers(None)

        successor, status = yield from self.run_rpc_safe(self.bootstrap_address, "rpc_find_successor_rec",
                                                         self.fingertable[0]["start"])
        # print("Looking for %s" % self.fingertable[0]["start"])
        self.fingertable[0]["successor"] = successor  # TODO: validate successor
        self.print_finger_table()

        # Fix references of our direct neighbors
        yield from self.update_neighbors()

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
                finger_successor, status = yield from self.run_rpc_safe(self.bootstrap_address, "rpc_find_successor_rec",
                                                                        finger_next["start"])
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
        else:
            print("Predecessor ID: -")

    @asyncio.coroutine
    def update_finger_table(self, origin_node, i):
        # Do not include self.id in contrast to original paper to abort the recursive call if starting
        # node tries to update itself
        if in_interval(origin_node["node_id"], self.id, self.fingertable[i]["successor"]["node_id"]):
            self.log.info("For finger %d: origin_node is %s; successor was %s",
                          i, origin_node, self.fingertable[i]["successor"]["node_id"])

            self.fingertable[i]["successor"] = origin_node
            # Only forward to predecessor if it is not the peer that started this update cascade
            if self.predecessor["node_id"] != origin_node["node_id"]:
                yield from self.run_rpc_safe(self.predecessor["node_address"],
                                             "rpc_update_finger_table", origin_node, i)
                #remote_peer = yield from self.container.connect(self.predecessor["node_address"])
                #yield from remote_peer.rpc_update_finger_table(origin_node, i)

    @asyncio.coroutine
    def update_neighbors(self):
        """ Update immediate neighbors.

        Update our successor's pointer to reference us as immediate predecessor
        (according to default Chord specification).
        Notify our direct predecessor about our presence. This allows to early stabilize
        its immediate successor finger[0] early.

        Requires that finger[0] is set properly.
        """
        # Fix predecessor reference on our immediate successor
        successor = self.fingertable[0]["successor"]
        # No other peers yet in the network -> no maintenance possible
        if successor["node_id"] == self.id:
            return
        update_pred, conn_err = yield from self.run_rpc_safe(successor["node_address"],
                                                             "rpc_update_predecessor", self.as_dict())
        if conn_err != 0:
            # Immediate successor is not responding
            self.log.warn("Immediate successor %s not responding.", successor)
            return  # TODO: better error handling

        self.log.debug("Predecessor update result: %s", update_pred)
        # TODO: validate input
        if update_pred["node_address"] == self.node_address and "old_predecessor" in update_pred:
            # Use successor node if chord overlay only has bootstrap node as only one
            # TODO: check whether predecessor is before our ID
            self.predecessor = update_pred["old_predecessor"]
            self.log.info("Set predecessor: %s", self.predecessor)

            # Notify our predecessor to be aware of us (new immediate successor)
            # It might already know. In that case, this call is useless.
            yield from self.run_rpc_safe(self.predecessor["node_address"], "rpc_update_successor",
                                         self.as_dict())

        elif update_pred["node_address"] != self.node_address:
            # Stabilize:
            # Seems as our successor reference is not correct anymore.
            # We trust our original successor that it tells the truth in this case.
            self.fingertable[0]["successor"] = update_pred
            self.log.info("Periodic fix: updated successor reference to node %d (%s)",
                          update_pred["node_id"], update_pred["node_address"])

        elif update_pred["node_address"] == self.node_address:
            self.log.info("Successor reference ok. Nothing to do.")

        else:
            # Something went wrong during update. This is only relevant if it happened during startup
            # of this node.
            # A new node might have joined in the meantime -> TODO: update our reference or clean exit
            self.log.error("Could not update predecessor reference of our successor. Try restarting.")

    @asyncio.coroutine
    def update_successor(self, new_node):
        """Updates the reference to our immediate successor during periodic maintenance or
        triggered by other peer's hint.

        A neighboring successor uses this function to notify us about its presence. This ensures
        that the Chord ring is correct.
        The parameter ``new_node`` gives a hint about the new successor in this case. To verify
        this hint, this node contacts its old successor.

        :param new_node:
            Successor hint.
        """
        old_successor = self.fingertable[0]["successor"]
        # No other peers yet in the network -> no maintenance possible
        if old_successor["node_id"] == self.id and new_node is None:
            return

        # New successor before old one or old one not responding anymore (last option is TODO)
        if in_interval(new_node["node_id"], self.id, old_successor["node_id"]):
            # Check old successor whether it already accepted new node
            # TODO: validation + timeout catch
            old_successor_view, peer_err = yield from self.run_rpc_safe(old_successor["node_address"],
                                                                        "rpc_get_node_info")
            if peer_err != 0:
                # Immediate successor is not responding
                self.log.warn("Immediate successor %s not responding.", old_successor)
                return  # TODO: better error handling, e.g., update on peer_err > 0

            if old_successor_view["predecessor"]["node_address"] == new_node["node_address"]:
                # Update finger table to point to new immediate successor
                self.fingertable[0]["successor"] = new_node
                self.log.info("Updated successor reference to node %d (%s)",
                              new_node["node_id"], new_node["node_address"])

            else:
                # Do not update, only mention suspicious observation
                self.log.error("Node %d (%s) wants to be our immediate successor, but original successor %d (%s) "
                               "does not reference it. Looks malicious. Or our view is not fresh anymore :(",
                               new_node["node_id"], new_node["node_address"],
                               old_successor["node_id"], old_successor["node_address"])

    @asyncio.coroutine
    def update_others(self):
        """Update peers' finger table that should refer to our node and notify them.
        """
        for k in range(0, CHORD_FINGER_TABLE_SIZE):
            id = (self.id - 2**k) % CHORD_RING_SIZE
            # Find predecessor
            successor = yield from self.find_successor(id, with_neighbors=True)
            print("TRACE: ", successor)
            p = successor["predecessor"]
            # In rare cases with id exactly matching the node's key, successor is more correct
            # Ex: 116 is looking for node 114 (finger 2), predecessor would be node 249 with successor 114
            #     In this case, finger in node 114 should be changed, too.
            #if p["successor"]["node_id"] == id:
            #    p = p["successor"]
            self.log.info("Update peer: %s", p)
            if self.id != p["node_id"]:
                yield from self.run_rpc_safe(p["node_address"], "rpc_update_finger_table",
                                             self.as_dict(), k)
                # remote_peer = yield from self.container.connect(p["node_address"])
                # yield from remote_peer.rpc_update_finger_table(self.as_dict(), k)

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

        if successor is None:
            # TODO: do live check
            self.log.warn("No suitable node found for start %d. Do not update finger.", cur_finger["start"])
        elif successor != cur_finger["successor"]:
            self.log.info("Finger %d updated: successor is now %s (old: %s)",
                          finger_id, successor, cur_finger["successor"])
            cur_finger["successor"] = successor
        # else:
        #     self.log.warn("Received successor for finger %d not fitting to ID ranges in finger table: %d not in [%d, %d)",
        #                   finger_id, successor["node_id"], cur_finger["start"], next_finger["start"])

    @asyncio.coroutine
    def check_predecessor(self):
        """Verifies this node's immediate predecessor's live. If it is lost, remove reference to give new nodes a
        chance to repair it.
        """
        if self.predecessor is None or self.predecessor["node_id"] == self.id:
            return

        predecessor, status = yield from self.run_rpc_safe(self.predecessor["node_address"],
                                                           "rpc_get_node_info")
        if status != 0 or \
                (status == 0 and predecessor["successor"]["node_address"] != self.node_address):
            # Predecessor not reachable anymore or our predecessor does not reference us -> Clean up.
            self.predecessor = None
            self.log.warn("Removing invalid predecessor reference.")

    @asyncio.coroutine
    def find_successor(self, node_id, with_neighbors=False):
        """Wrapper for :func:`find_successor_rec` to clean responses.

        :param node_id:
            Key ``node_id`` whose responsible successor is interesting.
        :param with_neighbors:
            If ``True``, the immediate successor and predecessor nodes augment the result of
            the responsible successor.
        :return:
            Responsible successor node for given key ``node_id``.
        :rtype: dict or None
        """
        result = yield from self.find_successor_rec(node_id, with_neighbors=with_neighbors)
        # Check for problems during lookup
        if "status" in result and result["status"] != 0:
            self.log.warn("Could not resolve responsible peer. Err: %s", result)
            result = None

        result = strip_node_response(result, immediate_neighbors=with_neighbors)
        return result

    @asyncio.coroutine
    def find_successor_trace(self, node_id):
        """Wrapper for :func:`find_successor_rec` with trace log of intermediate hops.

        :param node_id:
        :return:
        """
        result = yield from self.find_successor_rec(node_id, tracing=True)
        result = strip_node_response(result, trace_log=True)
        return result

    @asyncio.coroutine
    def find_successor_rec(self, node_id, with_neighbors=False, tracing=False):
        """Recursive find successor. Used locally and by remote peers.

        :param node_id:
            Key ``node_id`` whose responsible successor is interesting.
        :param with_neighbors:
            If ``True``, the immediate successor and predecessor nodes augment the result of
            the responsible successor.
        :return:
            Responsible successor node for given key ``node_id``.
        """
        successor = self.fingertable[0]["successor"]
        if in_interval(node_id, self.id, successor["node_id"], inclusive_right=True):
            # Augment node with infos about its successor (and its predecessor)
            # This also allows to check whether this node is still alive
            # TODO: also do live check if no details are needed
            successor_details = successor.copy()
            if with_neighbors:
                # TODO: validation
                successor_details, status = yield from self.run_rpc_safe(successor["node_address"], "rpc_get_node_info")
                if status != 0:
                    successor_details.update({"status": 1, "message": "last hop not responding"})

            # Add list for tracing (last hop is already included in the return message)
            if tracing:
                successor_details["trace"] = []

            return successor_details

        else:
            # Find closest finger to node_id and forward recursive query
            # If the current finger's node does not respond, choose a less optimal one.
            # TODO: remember faulty nodes and replace if it happens too often
            this_node = self.as_dict()
            i = 1

            next_hop = self.get_closest_preceding_finger(node_id, fall_back=0)
            while next_hop != this_node:
                print("[find_successor_rec] Closest finger node for %d: %s" % (node_id, next_hop))

                # TODO: validate and check for None
                peer_data, status = yield from self.run_rpc_safe(next_hop["node_address"], "rpc_find_successor_rec",
                                                                 node_id, with_neighbors=with_neighbors, tracing=tracing)
                if status == 0:
                    print("[find_successor_rec] Remote result for id %d: %s" % (node_id, peer_data))

                    # Tracing
                    # If the recursion tree is built completely, the touched peers are inserted in a trace list on
                    # the way back.
                    # The preceding node inserts its next hop in the trace. This provides a basic protection that a
                    # malicious node cannot prevent being visible in the list.
                    if tracing:
                        if peer_data is None:
                            peer_data = {"status": 1, "message": "trace incomplete."}
                        peer_data["trace"].append(next_hop)

                    return peer_data

                print("[find_successor_rec] Remote id %d with '%s' failed. Try next [%d]." %
                      (next_hop["node_id"], next_hop["node_address"], i))

                next_hop = self.get_closest_preceding_finger(node_id, fall_back=i)
                i += 1

            # Already reached end of unique peers in our finger table: we are isolated right now
            self.log.info("No suitable alternatives as next hop.")
            return {"status": 1, "message": "no suitable alternatives found, giving up."}

    # @asyncio.coroutine
    # def find_successor_rec(self, node_id):
    #     """ Iterative find successor
    #
    #     .. warning::
    #        Deprecated: use recursive :func:`find_successor_rec` instead.
    #     """
    #     successor = self.fingertable[0]["successor"]
    #     if in_interval(node_id, self.id, successor["node_id"], inclusive_right=True):
    #         return successor
    #     else:
    #         node = yield from self.find_predecessor(node_id)
    #         print("[find_successor_rec] Calculated node for %d: %s" % (node_id, node))
    #         return node["successor"]  # Attention: relies on available successor information which has to be
    #                                   # retrieved by closest_preceding_finger()

    @asyncio.coroutine
    def find_predecessor(self, node_id):
        """Find predecessor

        .. warning::
           Deprecated: use :func:`find_successor_rec` instead. It also contains a reference to the node's predecessor.
        """
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

    def get_closest_preceding_finger(self, node_id, fall_back=0, start_offset=CHORD_FINGER_TABLE_SIZE-1):
        """
        Find closest preceding finger within m -> 0 fingers.

        :param node_id:
            node ID as an integer.
        :param fall_back:
            chooses less optimal finger nodes if value increases.

            This allows to find a slower, but still working lookup although the best matching finger
            is not responding anymore.
            In the worst case, this function falls back to this node itself. For example, this is the
            case if our immediate successor is responsible for all of our fingers, but does not respond
            to requests done previously.

        :return:
            returns the interesting node descriptor as a dictionary with successor and predecessor.
        :rtype: dict
        """
        prev_successor = None

        for k in range(start_offset, -1, -1):
            finger = self.fingertable[k]
            finger_successor = self.fingertable[k]["successor"]
            self.log.debug("Iterate finger %d: %d in %s", k, node_id, self.fingertable[k])

            # Alternative: find entry with node_id > finger["start"] and already contact this node.
            # In all cases, it will fall back to a less optimal predecessor if this node does not respond.
            # Advantage: can reduce hops to a destination and is more stable in our 8-bit fingers test environment.
            #if in_interval(finger["start"], self.id, node_id, inclusive_right=True):
            if in_interval(finger_successor["node_id"], self.id, node_id):
                if fall_back == 0:
                    return finger_successor
                else:
                    if prev_successor is not None and prev_successor != finger_successor["node_address"]:
                        fall_back -= 1

                    prev_successor = finger_successor["node_address"]
                    continue

        return self.as_dict()

    @asyncio.coroutine
    def stabilize(self):
        while self.activated:
            yield from asyncio.sleep(self.fix_interval)
            self.log.info("Running periodic fix up.")

            # Assure that successor still references us as immediate predecessor
            # yield from self.update_successor()
            yield from self.update_neighbors()
            # Update fingers 1 -> m one after each other (finger[0] managed by update_successor)
            self.fix_next = max(1, (self.fix_next + 1) % CHORD_FINGER_TABLE_SIZE)
            yield from self.fix_finger(self.fix_next)
            # Check predecessor and remove reference if wrong
            yield from self.check_predecessor()

            print("Current finger table:")
            self.print_finger_table()
            print("Stored entries: ", len(self.storage.data))

    @asyncio.coroutine
    def put_data(self, key, data, ttl, replicationCount=3):
        replica = Replica(CHORD_RING_SIZE)

        keys = replica.get_key_list(key, replicationCount)

        print("\n\n\n\nPUT KEYS ARE ", keys) # [197, 210, 70]
        successes = 0
        for keyWithReplicaIndex in keys:
            storage_node = yield from self.find_successor(keyWithReplicaIndex)
            print("Found successor for storage: ", storage_node)

            if storage_node["node_id"] == self.id:
                self.storage.put(keyWithReplicaIndex, data, ttl=ttl)
                successes += 1
            else:
                # Directly connect to remote peer and store it there
                # TODO: validate
                result, status = yield from self.run_rpc_safe(storage_node["node_address"],
                                                              "rpc_dht_put_data", keyWithReplicaIndex, data, ttl)
                if result["status"] == 0:
                    successes += 1
                else:
                    pass # TODO: FAIL MESSAGE

        print("\n\n\n\PUTS OK: ", successes)
        if successes >= 1:
            return {
                "status": 0,
                "successes" : successes
            }
        else:
            return {
                "status": 1,
                "successes" : successes,
                "messages" : "Data could not be saved."
            }

    @asyncio.coroutine
    def get_data(self, key):
        replica = Replica(CHORD_RING_SIZE)
        keys = replica.get_key_list(key, 1) # 5 is the replications that are tried before abort

        for keyWithReplicaIndex in keys:
            storage_node = yield from self.find_successor(keyWithReplicaIndex)
            print("got storage_node: ", storage_node)
            if storage_node["node_id"] == self.id:
                return {
                    "status": 0,
                    "data": self.storage.get(keyWithReplicaIndex)
                }

            else:
                # Directly connect to remote peer and fetch data from there
                # TODO: validate
                result, status = yield from self.run_rpc_safe(storage_node["node_address"],
                                                              "rpc_dht_get_data", keyWithReplicaIndex)
                if result["status"] == 0:
                    return result
                else:
                    print("result ERROR", result)

        return {"status": 1, "data": []}

    @asyncio.coroutine
    def get_trace(self, key):
        nodes = yield from self.find_successor_trace(key)
        print("Get_trace result:", nodes)

        print("Hop 0: node %s", nodes["node_id"])
        for hop_index, node in enumerate(nodes["trace"]):
            print("Hop %d : node %s" % (hop_index, node))


    ##########################################################################
    ### RPC wrappers and functions for maintaining Chord's network overlay ###
    @asyncio.coroutine
    def run_rpc_safe(self, remote_address, func_name, *args, **kwargs):
        data = None
        err = 1
        try:
            #print("Before container.connect()")
            fut_peer = self.container.connect(remote_address)
            remote_peer = yield from asyncio.wait_for(fut_peer, timeout=self.network_timeout)
            #print("After connect()")
            # Invoke remote function
            data = yield from getattr(remote_peer, func_name)(*args, **kwargs)
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

        except ValidationError as ex:
            err = 1
            self.log.error("Schema validation error: %s", str(ex))
            traceback.print_exc()

        except SchemaError as ex:
            err = 1
            self.log.error("Schema validation error: %s", str(ex))
            traceback.print_exc()

        except Exception as ex:
            err = 1
            self.log.error("Unhandled error during RPC function %s to %s: %s", func_name, remote_address, ex)
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

        validate(remote_node, SCHEMA_UPDATE_PREDECESSOR) # validata schema

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
            # No change for this node's predecessor, because it is closer to our node than the asking peer.
            # Its live is checked periodically by ``check_predecessor``.
            return self.predecessor

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
    def rpc_find_successor_rec(self, node_id, with_neighbors=False, tracing=False):

        # TODO: validate params to prevent attacks!
        res = yield from self.find_successor_rec(node_id, with_neighbors=with_neighbors, tracing=tracing)
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
                "status": 1,
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
                "status": 1
            }
    @asyncio.coroutine
    def test_stresstest(self, message):

        message = "ok"

        # force some exceptions
        try:
            yield from self.rpc_update_predecessor({"node_id": 213}) # missing node address
        except Exception as e:
            message = str(e)

        return message.encode("utf-8")

        # if message == TESTMESSAGES_MESSAGE_FAKE_WRONGVALUE:
        #     return "test wrong value ok".encode("utf-8")
        # if message == TESTMESSAGES_MESSAGE_FAKE_MISSINGVALUE:
        #     return "test missing value ok".encode("utf-8")

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
        res_node = yield from remote_agent.rpc_find_successor_rec(self.id + 1)
        print("%s got answer from %s: my successor is %s" % (self.node_address, addr, str(res_node)))
