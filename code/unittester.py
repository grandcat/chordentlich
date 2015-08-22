#!/usr/bin/python3
from helpers.test_iniParser import *
from helpers.test_messageParser import *
from helpers.test_replica import *
from helpers.test_storage import *
from helpers.test_validator import *


import logging
if __name__ == '__main__':
    test_classes_to_run = [TestIniParser, TestValidator, TestStorage, TestReplica, TestMessageParser]

    loader = unittest.TestLoader()

    suites_list = []
    for test_class in test_classes_to_run:
        log = logging.getLogger( "chord.log" )
        print( "[LOAD TEST CLASS]", str(test_class))
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    big_suite = unittest.TestSuite(suites_list)

    runner = unittest.TextTestRunner()
    results = runner.run(big_suite)
