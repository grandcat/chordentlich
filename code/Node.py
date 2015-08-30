#!/usr/bin/python3
import asyncio
import random
import traceback
import aiomas
import hashlib
import logging
import errno

from helpers.validator import *
from helpers.chordInterval import *
from helpers.storage import Storage
from helpers.replica import Replica
from helpers.messageDefinitions import *
from jsonschema import validate, Draft3Validator
from jsonschema.exceptions import ValidationError, SchemaError


def filter_node_response(data, immediate_neighbors=False, trace_log=False):
    if data is None:
        return None

    output = {
        "node_id": data["node_id"],
        "node_address": data["node_address"]
    }
    if immediate_neighbors and "successor" in data:
        output["successor"] = filter_node_response(data["successor"])
    if immediate_neighbors and "predecessor" in data:
        output["predecessor"] = filter_node_response(data["predecessor"])
    if trace_log:
        output["trace"] = data["trace"]

    return output


class Node(aiomas.Agent):
    """
    Node
    """

    class Successor:
        def __init__(self, finger_table_ref):
            self.list = []
            self._backup = None     # List backup before ``update_others``
            self.max_entries = 3

            self._fingertable = finger_table_ref

        def set(self, new_successor, replace_old=False):
            if len(self.list) == 0:
                self.list = [new_successor]
            else:
                self.list[0] = new_successor

            # Maintain first finger to represent correct successor
            self._correct_finger_table(new_successor, replace_old=replace_old)
            # self._fingertable[0]["successor"] = new_successor

        def get(self):
            return self.list[0]

        def update_others(self, successors, ignore_key=-1):
            if successors:
                self._backup = self.list
                self.list = [self.get()] + [x for x in successors if x["node_id"] != ignore_key]
                del self.list[self.max_entries:]

            else:
                print("[Node:update_others] Not able to update successor list based on input.")

        def revert_update(self):
            self.list = self._backup

        def delete_first(self):
            del self.list[0]
            self._correct_finger_table(self.get(), replace_old=True)

        def count_occurrence(self, successor):
            # Increase efficiency by using collections.OrderedDict with last=False
            return self.list.count(successor)

        def _correct_finger_table(self, new_successor, replace_old=False, offset=0):
            old_peer = self._fingertable[offset].get("successor")
            self._fingertable[offset]["successor"] = new_successor

            if old_peer is None or not replace_old:
                return

            for entry in self._fingertable[offset+1:]:
                # if entry["successor"] is None:
                #     break
                if entry and entry["successor"].get("node_id") == old_peer["node_id"]:
                    entry["successor"] = new_successor
                else:
                    break

        def print_list(self, pre_text="Successor list"):
            print(pre_text)
            print("   ID   |   Address")
            print("----------------------")
            for successor in self.list:
                print("%s %s" % (str(successor["node_id"]).ljust(9), successor["node_address"]))
            print("")

    def __init__(self, container, node_address):
        # Async RPC init
        super().__init__(container, node_address)
        self.node_address = node_address

        self.log = logging.getLogger(__name__)
        self.log.info("Node server listening on %s.", node_address)

        # Node state
        self.bootup_finished = False
        self.activated = True
        self.network_timeout = 7
        self.storage = Storage()
        # Wide-range Overlay network
        self.fingertable = []
        self.fix_interval = 4 + random.randint(0, 5)
        self.fix_next = 0
        # Short-range Successor list (manages finger[0] in fingertable)
        self.successor = Node.Successor(self.fingertable)

    @asyncio.coroutine
    def _check_running_state(self):
        """
        Delay operation if booting process is not finished yet.

        This assures that internal data structures are not accessed before.
        """
        while not self.bootup_finished:
            self.log.info("Delaying request. Bootup not finished.")
            yield from asyncio.sleep(1)

    def as_dict(self, serialize_neighbors=False, additional_data=False):
        dict_node = {
            "node_id": self.id,
            "node_address": self.node_address,
        }
        if serialize_neighbors:
            dict_node["successor"] = self.successor.get()
        if serialize_neighbors and self.predecessor:
            dict_node["predecessor"] = self.predecessor
        if additional_data:
            dict_node["additional_data"] = self.additional_data

        return dict_node

    @staticmethod
    def generate_key(address):
        # TODO: public key hash instead of protocol + IP address + port
        # TODO: remove modulo
        return int(hashlib.sha256(address.encode()).hexdigest(), 16) % CHORD_RING_SIZE

    @asyncio.coroutine
    def join(self, node_id=None, node_address=None, bootstrap_address=None, additional_data=None):
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
        self.node_address = node_address or self.node_address   # normally already set in __init__
        self.bootstrap_address = bootstrap_address
        self.predecessor = None
        self.log.info("[Configuration] node_id: %d, bootstrap_node: %s", self.id, self.bootstrap_address)

        self.additional_data = additional_data or {}

        if self.bootstrap_address:
            # Regular node joining via bootstrap node
            self.__generate_fingers(None)

            # Try joining later if our successor does not respond
            successor = None
            while True:
                successor, status = yield from self.run_rpc_safe(self.bootstrap_address, "rpc_find_successor_rec",
                                                                 self.fingertable[0]["start"])
                if status == 0:
                    if successor["status"] == 0:
                        # Successors seems to be reachable: we can proceed
                        break
                    else:
                        self.log.warn("Successor node not responding.")
                else:
                    self.log.warn("Bootstrap node not responding.")

                self.log.warn("Will retry in 3 seconds.")
                yield from asyncio.sleep(3)

            # Proceed with a working successor
            successor = filter_node_response(successor)
            self.successor.set(successor)

            yield from self.init_successor_list(successor)
            yield from self.init_finger_table()
            self.bootup_finished = True
            yield from self.update_others()

        else:
            # This is the bootstrap node
            successor_node = self.as_dict()
            self.__generate_fingers(successor_node)
            self.successor.set(successor_node)  # bootstrap first references itself
            self.bootup_finished = True

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
        self.print_finger_table()

        # Fix references to our direct neighbors
        # This is necessary that find_successor works correctly.
        yield from self.update_neighbors(initialization=True)

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
                finger_next["successor"] = filter_node_response(finger_successor)

        # Optimization for joining node (if not bootstrap node)
        # - Find close node to myself (e.g., successor)
        # - Request finger table and store temporary_entries
        # - for each of my needed finger table starts, use closest entries and directly ask this node.
        # - Fallback to node asked previously (or bootstrap node as last fallback) if node is not responding

    def __generate_fingers(self, successor_reference):
        for k in range(0, CHORD_FINGER_TABLE_SIZE):
            entry = {
                "start": ((self.id + 2**k) % CHORD_RING_SIZE),
                "successor": successor_reference
            }
            # TODO: add successor if not bootstrap node
            self.fingertable.append(entry)

        self.log.debug("Default finger table: %s", str(self.fingertable)+"\n\n")


    def print_finger_table(self, fingerTableToPrint=None):
        if not fingerTableToPrint:
            fingerTableToPrint = self.fingertable

        print(" START  |   ID ")
        print("-----------------------")
        for tableEntry in fingerTableToPrint:
            if tableEntry["successor"]:
                print("%s  %s" % (str(tableEntry["start"]).ljust(4), tableEntry["successor"]["node_id"]))
            else:
                print(str(tableEntry["start"]).ljust(4) + "  -  ")

        if self.predecessor:
            print("Predecessor ID: %d \n" % self.predecessor["node_id"])
        else:
            print("Predecessor ID: - \n")

    @asyncio.coroutine
    def init_successor_list(self, successor):
        """Fetch successor list from our immediate successor when joining a network.
        """
        successor_details, status = yield from self.run_rpc_safe(successor["node_address"], "rpc_get_node_info",
                                                                 successor_list=True)

        self.successor.update_others(successor_details.get("successor_list"), self.id)
        self.log.info("New successor list: %s", self.successor.list)

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

    @asyncio.coroutine
    def update_neighbors(self, initialization=False):
        """ Update immediate neighbors.

        Update our successor's pointer to reference us as immediate predecessor
        (according to default Chord specification).
        Notify our direct predecessor about our presence. This allows to early stabilize
        its immediate successor finger[0].

        Requires that finger[0] is set properly.
        """
        successor = self.successor.get()
        if successor["node_id"] == self.id:
            return

        # Fix predecessor reference on our immediate successor
        update_pred, conn_err = yield from self.run_rpc_safe(successor["node_address"], "rpc_update_predecessor",
                                                             self.as_dict())
        if conn_err != 0:
            # Immediate successor is not responding (should not happen as checked before)
            self.log.warn("Immediate successor %s not responding.", successor)
            return  # TODO: better error handling

        self.log.debug("Predecessor update result: %s", update_pred)
        if update_pred["node_address"] == self.node_address and "old_predecessor" in update_pred:
            # Successfully integrated into Chord overlay network
            # Successor already references us at this point.
            if initialization:
                self.predecessor = filter_node_response(update_pred["old_predecessor"])
                self.log.info("Set predecessor: %s", self.predecessor)

                # Notify our predecessor to be aware of us (new immediate successor)
                # It might already know. In that case, this call is useless.
                # However, for a Chord network only consisting of one node, this is crucial that this node's
                # successor references us. Only like this, the circle is closed in the forward direction.
                yield from self.run_rpc_safe(self.predecessor["node_address"], "rpc_update_successor",
                                             self.as_dict())

            # Merge received key,values into own storage
            print("Keys received:", update_pred.get("storage"))
            self.storage.merge(update_pred.get("storage"))

        # elif update_pred["node_address"] != self.node_address:
        #     # Fix concurrent joins in the same area:
        #     # Seems that our successor got a closere predecessor in the mean time.
        #     # We trust our original successor that it tells the truth and correct our successor reference.
        #     new_successor = filter_node_response(update_pred)
        #
        #     if in_interval(new_successor["node_id"], self.id, successor["node_id"]):
        #         self.successor.set(new_successor)
        #         self.log.info("Periodic fix: updated successor reference to node %d (%s)",
        #                       new_successor["node_id"], new_successor["node_address"])
        #
        #         # Notify our new successor to change its predecessor reference to us
        #         # If this successor is still not the right one, it will be corrected in the next round.
        #         yield from self.run_rpc_safe(new_successor["node_address"], "rpc_update_predecessor",
        #                                      self.as_dict())
        #
        #     else:
        #         self.log.warn("Could not stabilize. Our original successors sends rubbish.")

        elif update_pred["node_address"] == self.node_address:
            self.log.info("Predecessor and successor references ok. Nothing to do.")

        else:
            # Something went wrong during update. This is only relevant if it happened during startup
            # of this node.
            # A new node might have joined in the meantime -> TODO: update our reference or clean exit
            print("[Update_neighbors] Response:", update_pred)
            print("[Update_neighbors] Myself:", self.as_dict())
            self.log.error("Could not update predecessor reference of our successor. Try restarting.")

    @asyncio.coroutine
    def update_successor(self, new_node):
        """Updates the reference to our immediate successor triggered by other peer's hint.

        A neighboring successor uses this function to notify us about its presence. This ensures
        that the Chord ring is correct.
        The parameter ``new_node`` gives a hint about the new successor in this case. To verify
        this hint, this node contacts its old successor.

        :param new_node:
            Successor hint.
        """
        old_successor = self.successor.get()
        # No other peers yet in the network -> no maintenance possible
        if old_successor["node_id"] == self.id and new_node is None:
            return

        # New successor before old one or old one not responding anymore (last option is TODO)
        if in_interval(new_node["node_id"], self.id, old_successor["node_id"]):
            # Check old successor whether it already accepted new node
            # TODO: validation + timeout catch
            successor_view, peer_err = yield from self.run_rpc_safe(old_successor["node_address"],
                                                                    "rpc_get_node_info")
            if peer_err != 0:
                # Immediate successor is not responding
                self.log.warn("Immediate successor %s not responding.", old_successor)
                return  # TODO: better error handling, e.g., update on peer_err > 0

            if successor_view["predecessor"]["node_address"] == new_node["node_address"]:
                # Update finger table to point to new immediate successor
                new_node = filter_node_response(new_node)
                self.successor.set(new_node)
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
            p = successor["predecessor"]
            # In rare cases with id exactly matching the node's key, successor is more correct to reduce hops.
            # Ex: 116 is looking for node 114 (finger 2), predecessor would be node 249 with successor 114
            #     In this case, finger in node 114 should be changed, too.
            # if p["successor"]["node_id"] == id:
            #     p = p["successor"]
            self.log.info("Update peer: %s", p)
            if self.id != p["node_id"]:
                yield from self.run_rpc_safe(p["node_address"], "rpc_update_finger_table",
                                             self.as_dict(), k)

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
            self.log.warn("No suitable node found for start %d. Do not update finger.", cur_finger["start"])

        elif successor != cur_finger["successor"]:
            self.log.info("Finger %d updated: successor is now %s (old: %s)",
                          finger_id, successor, cur_finger["successor"])
            cur_finger["successor"] = filter_node_response(successor)
        # else:
        #     self.log.warn("Received successor for finger %d not fitting to ID ranges in finger table: %d not in [%d, %d)",
        #                   finger_id, successor["node_id"], cur_finger["start"], next_finger["start"])

    @asyncio.coroutine
    def update_successor_list(self):
        """Periodically checks availability of our successor peer, maintains a list of possible successors
        and swaps to another successor if the first fails.
        """
        if len(self.successor.list) == 0 or self.successor.get() == self.as_dict():
            return

        while len(self.successor.list) > 0:
            cur_successor = self.successor.get()

            # Query our successor about its current successor list
            successor_details, status = yield from self.run_rpc_safe(cur_successor["node_address"], "rpc_get_node_info",
                                                                     successor_list=True)
            if status == 0:
                # TODO: filter successor_details
                self.successor.print_list()

                self.successor.update_others(successor_details["successor_list"], ignore_key=self.id)
                # Predecessor of a successor can be missing (None)
                new_successor = successor_details.get("predecessor")
                print("[update_successor_list] New successor would be:", new_successor)

                if new_successor and in_interval(new_successor["node_id"], self.id, cur_successor["node_id"]):
                    # Our successor already has a different and closer predecessor than us
                    new_successor, status = yield from self.run_rpc_safe(new_successor["node_address"], "rpc_get_node_info",
                                                                         successor_list=True)
                    print("[update_successor_list] SPECIAL CASE: would move to:", new_successor)
                    if status == 0 and "successor_list" in new_successor:
                        # Linking to the new peer being our successor now.
                        print("update_successor_list] SPECIAL CASE: moved to new successor")
                        self.successor.set(filter_node_response(new_successor))
                        self.successor.update_others(new_successor["successor_list"], ignore_key=self.id)
                        # Successor view must contain at least our previous successor in its list.
                        # Otherwise, this peer seems to behave strange
                        if self.successor.count_occurrence(cur_successor) == 0:
                            self.log.warn("Reverting successor list as new successor does not include previous one. "
                                          "Looks suspicious to me.")
                            self.successor.revert_update()

                # Notify our successor here to accelerate the stabilization
                yield from self.update_neighbors()

                break

            else:
                # Try next successor as current one does not respond appropriate
                self.log.info("Successor ID %d not responding. Trying next.", self.successor.get()["node_id"])
                if len(self.successor.list) > 1:
                    self.successor.delete_first()
                else:
                    self.log.warn("No evidence of any other peers alive. Going over to act as bootstrap for others")
                    self.successor.set(self.as_dict())

    @asyncio.coroutine
    def check_predecessor(self):
        """Verifies this node's immediate predecessor's live. If it is lost, remove reference to give new nodes a
        chance to repair it.
        """
        if self.predecessor is None or self.predecessor["node_id"] == self.id:
            return

        predecessor, status = yield from self.run_rpc_safe(self.predecessor["node_address"],
                                                           "rpc_get_node_info")
        print("[check_predecessor] Connected to pred: %s" % predecessor)
        print("[check_predecessor] Previous pred was: %s" % self.predecessor)

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

        result = filter_node_response(result, immediate_neighbors=with_neighbors)
        return result

    @asyncio.coroutine
    def find_successor_trace(self, node_id):
        """Wrapper for :func:`find_successor_rec` with trace log of intermediate hops.

        :param node_id:
        :return:
        """
        result = yield from self.find_successor_rec(node_id, tracing=True)
        result = filter_node_response(result, trace_log=True)
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
        successor = self.successor.get()
        if in_interval(node_id, self.id, successor["node_id"], inclusive_right=True):
            # Check live of successor node and augment its information with successor and predecessor links
            # if required
            successor_details = successor.copy()
            successor_neighborhood, status = yield from self.run_rpc_safe(successor["node_address"], "rpc_get_node_info",
                                                                          additional_data=tracing)
            if status == 0:
                # Successor node is alive
                if with_neighbors:
                    successor_details.update(filter_node_response(successor_neighborhood, immediate_neighbors=True))

                successor_details["status"] = 0
            else:
                # Successor node is dead
                successor_details.update({"status": 1, "message": "last hop not responding"})

            # Add list for tracing
            if tracing:
                last_hop = successor.copy()
                last_hop.update({"additional_data": successor_neighborhood.get("additional_data", {}) if successor_neighborhood else {}})
                successor_details["trace"] = [last_hop]
                # Include our own additional data to be integrated by our preceding hop
                successor_details["additional_data"] = self.additional_data

            return successor_details

        else:
            # Find closest finger to node_id and forward recursive query.
            # If the current finger's node does not respond, try a less optimal one -> requires more hops.
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
                    # Regarding the order, the goal peer is at position 0 in the list and the first hop from the sender
                    # is at the last position n-1 (n describes all involved nodes).
                    if tracing:
                        if peer_data is None:
                            peer_data = {"status": 1, "message": "trace incomplete."}

                        successor_node = next_hop.copy()
                        successor_node["additional_data"] = peer_data["additional_data"]
                        peer_data["trace"].append(successor_node)

                    return peer_data

                print("[find_successor_rec] Remote id %d with '%s' failed. Try next [%d]." %
                      (next_hop["node_id"], next_hop["node_address"], i))

                next_hop = self.get_closest_preceding_finger(node_id, fall_back=i)
                i += 1

            # Already reached end of unique peers in our finger table: we are isolated right now
            self.log.info("No suitable alternatives as next hop.")
            return {"status": 1, "message": "no suitable alternatives found, giving up."}

    # @asyncio.coroutine
    # def find_successor(self, node_id):
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
    #
    # @asyncio.coroutine
    # def find_predecessor(self, node_id):
    #     """Find predecessor
    #
    #     .. warning::
    #        Deprecated: use :func:`find_successor_rec` instead. It also contains a reference to the node's predecessor.
    #     """
    #     selected_node = self.as_dict(serialize_neighbors=True)
    #     previous_selected_node = None
    #
    #     while not in_interval(node_id, selected_node["node_id"], selected_node["successor"]["node_id"], inclusive_right=True):
    #         self.log.info("Node ID %d not in interval (%d, %d]",
    #                       node_id,
    #                       selected_node["node_id"],
    #                       selected_node["successor"]["node_id"])
    #         if selected_node["node_id"] == self.id:
    #             # Typically in first round: use our finger table to locate close peer
    #             print("Looking for predecessor of %d in first round." % node_id)
    #             selected_node = yield from self.get_closest_preceding_finger(node_id)
    #
    #             print("Closest finger: %s" % selected_node)
    #             # If still our self, we do not know closer peer and should stop searching
    #             # if selected_node["node_id"] == self.id:
    #             #     break
    #
    #         else:
    #             # For all other remote peers, we have to do a RPC here
    #             self.log.debug("Starting remote call.")
    #             peer = yield from self.container.connect(selected_node["node_address"])
    #             selected_node = yield from peer.rpc_get_closest_preceding_finger(node_id)
    #             # TODO: validate received input before continuing the loop
    #             self.log.info("Remote closest node for ID %d: %s", node_id, str(selected_node))
    #
    #         # Detect loop without progress
    #         if previous_selected_node == selected_node:
    #             self.log.error("No progress while looking for node closer to ID %d than node %s", node_id, selected_node)
    #             raise aiomas.RemoteException("Remote peer did not return more closer node to given Id " + str(node_id), "")
    #         previous_selected_node = selected_node
    #
    #     return selected_node

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
            # if in_interval(finger["start"], self.id, node_id, inclusive_right=True):
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
        yield from self._check_running_state()

        while self.activated:
            yield from asyncio.sleep(self.fix_interval)
            self.log.info("Running periodic fix up.")

            print("Current finger table:")
            self.print_finger_table()
            self.log.info("[This node] %s", self.as_dict())
            print("Stored entries: ", len(self.storage.data))

            # Assure that successor still references us as immediate predecessor
            yield from self.update_successor_list()
            # yield from self.update_neighbors()  # called in update_successor_list
            # Update fingers 1 -> m one after each other (finger[0] managed by update_successor)
            self.fix_next = max(1, (self.fix_next + 1) % CHORD_FINGER_TABLE_SIZE)
            yield from self.fix_finger(self.fix_next)
            # Check predecessor and remove reference if wrong
            yield from self.check_predecessor()


    @asyncio.coroutine
    def put_data(self, key, data, ttl, replication_count=-1):
        replica = Replica(CHORD_RING_SIZE)

        keys = replica.get_key_list(key, replicationCount=replication_count)

        print("\n\n\n\nPUT KEYS ARE ", keys)  # [197, 210, 70]
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
                    pass  # TODO: FAIL MESSAGE

        print("\n\n\n\PUTS OK: ", successes)
        if successes >= 1:
            return {
                "status": 0,
                "successes": successes
            }
        else:
            return {
                "status": 1,
                "successes": successes,
                "messages": "Data could not be saved."
            }

    @asyncio.coroutine
    def get_data(self, key, replication_count=-1):
        replica = Replica(CHORD_RING_SIZE)
        keys = replica.get_key_list(key, replicationCount=replication_count)  # 3 is the replications that are tried before abort

        for keyWithReplicaIndex in keys:
            storage_node = yield from self.find_successor(keyWithReplicaIndex)
            print("got storage_node:", storage_node)

            if storage_node.get("node_id") == self.id:
                # Note the case that this node received the responsibility for a failed node.
                # Given that the missing data might not be available on this node, continue the replica loop.
                result = self.rpc_dht_get_data(keyWithReplicaIndex)
                print("[rpc_dht_get_data] Result is:", result)
                if result["status"] == 0:
                    return result

            else:
                # Directly connect to remote peer and fetch data from there
                # TODO: validate
                result, status = yield from self.run_rpc_safe(storage_node.get("node_address"),
                                                              "rpc_dht_get_data", keyWithReplicaIndex)
                if status == 0 and result["status"] == 0:
                    return result
                else:
                    print("result ERROR", result)

        # Lookup was not successful. Try locating other replica.
        return {"status": 1, "data": []}

    @asyncio.coroutine
    def get_trace(self, key):
        """Information about the hops involved in the path for the lookup of the given ``key``.

        The list is in reverse order:
        The target peer is at index 0. The node that started the request, is at the last position.

        :param key:
            Node ID to lookup.
        :return:
            Array with dicts containing the address information of all involved hops.
        """
        nodes = yield from self.find_successor_trace(key)
        print("Get_trace result:", nodes)
        trace_list = nodes["trace"]

        # Add our self as last hop to the list
        trace_list.append(self.as_dict(additional_data=True))

        for hop_index, node in enumerate(trace_list):
            print("Hop %d : node %s" % (len(trace_list) - hop_index - 1, node))

        return trace_list


    ##########################################################################
    ### RPC wrappers and functions for maintaining Chord's network overlay ###
    @asyncio.coroutine
    def run_rpc_safe(self, remote_address, func_name, *args, **kwargs):
        if remote_address is None or func_name is None:
            return None, errno.EINVAL

        data = None
        err = 1
        try:
            fut_peer = self.container.connect(remote_address)
            remote_peer = yield from asyncio.wait_for(fut_peer, timeout=self.network_timeout)
            # Invoke remote function
            data = yield from getattr(remote_peer, func_name)(*args, **kwargs)
            # Validate schema
            validate(data, SCHEMA_OUTGOING_RPC[func_name])
            err = 0

        except (asyncio.TimeoutError, asyncio.CancelledError):
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
            err = 2
            self.log.error("Validation error: %s", str(ex))
            data = None

        except SchemaError as ex:
            err = 1
            data = None
            self.log.error("Schema validation error: %s", str(ex))

        except Exception as ex:
            err = 1
            data = None
            self.log.error("Unhandled error during RPC function %s to %s: %s", func_name, remote_address, ex)
            traceback.print_exc()

        return data, err

    @aiomas.expose
    def rpc_get_node_info(self, successor_list=False, additional_data=False):
        node_info = self.as_dict(serialize_neighbors=True, additional_data=additional_data)
        if successor_list:
            node_info["successor_list"] = self.successor.list

        return node_info

    # @aiomas.expose
    # def rpc_get_fingertable(self):
    #     # Check: might be critical to be published completely
    #     return self.fingertable

    @aiomas.expose
    def rpc_update_predecessor(self, remote_node):
        yield from self._check_running_state()

        if not isinstance(remote_node, dict):
            raise TypeError('Invalid type in argument.')

        remote_id = remote_node["node_id"]
        # TODO: connect old predecessor if new node ID is not closer to us
        if self.predecessor is None or in_interval(remote_id, self.predecessor["node_id"], self.id):
            # If this is a bootstrap node and this is the first node joining,
            # set predecessor of new node to us. Like this, the ring topology is properly maintained
            old_predecessor = self.predecessor or self.as_dict()
            self.predecessor = filter_node_response(remote_node)
            self.log.info("Predecessor now links to requester %s (old: %s)", remote_node, old_predecessor)

            res = self.predecessor.copy()
            res["old_predecessor"] = old_predecessor

            # Get storage between old and new node. Delete data from our node.
            res["storage"] = self.storage.get_storage_data_between(old_predecessor["node_id"], remote_id)
            self.storage.delete_storage_data_between(old_predecessor["node_id"], remote_id)

            return res
        else:
            # No change for this node's predecessor, because it is closer to our node than the asking peer.
            # Its live is checked periodically by ``check_predecessor``.
            return self.predecessor

    @aiomas.expose
    def rpc_update_successor(self, node_hint):
        yield from self._check_running_state()

        if not isinstance(node_hint, dict):
            raise TypeError('Invalid type in argument.')

        yield from self.update_successor(node_hint)

    @aiomas.expose
    def rpc_update_finger_table(self, origin_node, i):
        yield from self._check_running_state()

        origin_node = filter_node_response(origin_node)
        validate(origin_node, SCHEMA_INCOMING_RPC["rpc_update_finger_table"])
        i = i % CHORD_RING_SIZE

        yield from self.update_finger_table(origin_node, i)
        return {"status": 0}

    @aiomas.expose
    def rpc_find_successor_rec(self, node_id, with_neighbors=False, tracing=False):
        yield from self._check_running_state()

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
            status = 0 if len(data) > 0 else 1
            return {
                "status": status,
                "data": data
            }
        else:
            return {
                "status": 1
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
        res_node = yield from remote_agent.rpc_find_successor_rec(self.id + 1)
        print("%s got answer from %s: my successor is %s" % (self.node_address, addr, str(res_node)))

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
