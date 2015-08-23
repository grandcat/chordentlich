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
