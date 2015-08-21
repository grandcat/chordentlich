def aiomas_parse_url(url):
        """Parse the agent *url* and return a ``((host, port), agent)`` tuple.

        Raise a :exc:`ValueError` if the URL cannot be parsed.

        This function is taken from the aiomas library (https://bitbucket.org/ssc/aiomas).
        """
        try:
            proto, addr_aid = url.split('://', 1)
            # assert proto in PROTOCOLS, '%s not in %s' % (proto, PROTOCOLS)

            if proto == 'tcp':
                addr, aid = addr_aid.split('/', 1)
                host, port = addr.rsplit(':', 1)
                if host[0] == '[' and host[-1] == ']':
                    # IPv6 addresses may be surrounded by []
                    host = host[1:-1]
                addr = (host, int(port))

            elif proto == 'ipc':
                assert addr_aid[0] == '['
                addr, aid = addr_aid[1:].split(']/', 1)

            assert aid, 'No agent ID specified.'

        except (AssertionError, IndexError, ValueError) as e:
            raise ValueError('Cannot parse agent URL "%s": %s' % (url, e))

        return addr, aid
