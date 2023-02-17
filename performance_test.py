from typing import Set
import psycopg2

import Logger
import time_handler
import db_handler as db
import index

import deduplicators.hash_xash_deduplicator as hxdd
import deduplicators.deduplicator as dedup

defaultSmallerRanges:list = [10000,20000,30000,40000,50000]
defaultBiggerRanges:list = [10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000]

def run_default_tests(conn, offset:int = 0, reps:int = 3, slowerAlgRanges:list = defaultSmallerRanges, fasterAlgRanges:list = defaultBiggerRanges):
    '''Executes a performance test of the algorithms developed in this thesis.
        "conn": the connection this test uses to retrieve the data from.
        "offset" an offset added to all id's retrieved.
        "reps": the amount of repretitions for each singular test.
        "slowerAlgRanges": the ranges of tables used to test slower algorithms like XASH-dedup.
        "fasterAlgRanges": the ranges of tables used to test faster algorithms like fnv1-dedup.'''
    
    # first execute all the slower algorithms
    if(slowerAlgRanges != None):
        
        # xash test
        Logger.log("XASH Test:")
        for tableAmount in slowerAlgRanges:
            tableDictWithSuperKeys = db.retrieveTestDataWithSuperKeysIdRange(conn, offset, offset+tableAmount)
            
            Logger.log("%s tables:"%tableAmount)
            
            for rep in range(reps):
                Logger.log("%s. measurement:"%(rep+1))
                duplicatesSet = hxdd.deduplicate(tableDictWithSuperKeys)
                Logger.log("")
        
        Logger.log("\n\n")    
        
        # simhash 64 bit test
        Logger.log("Simhash 64 bit Test:")
        for tableAmount in slowerAlgRanges:
            tableDict = db.retrieveTestDataIdRange(conn, db.CorpusType.MATE_MAIN, offset, offset+tableAmount)
            
            # for all possible toString versions
            for strVersion in index.ToStringVersion:
                Logger.log("%s tables with %s index:"%(tableAmount, strVersion.value))
                for rep in range(reps):
                    Logger.log("%s. measurement:"%(rep+1))
                    duplicatesSet = dedup.deduplicate(tableDict, strVersion, index.HashVersion.SIMILARITY_64_BIT)
                    Logger.log("")
        
        Logger.log("\n\n") 
    
    # then execute all the faster algorithms
    if(fasterAlgRanges != None):
    
        # the faster algs are executed in one loop
        for tableAmount in fasterAlgRanges:
            tableDict = db.retrieveTestDataIdRange(conn, db.CorpusType.MATE_MAIN, offset, offset+tableAmount)
                    
            # simhash 128 bit test
            for strVersion in index.ToStringVersion:
                Logger.log("\nRunning 128 bit %s simhash with %s tables:"%(strVersion.value, tableAmount))
                for rep in range(reps):
                    Logger.log("%s. measurement:"%(rep+1))
                    duplicatesSet = dedup.deduplicate(tableDict, strVersion, index.HashVersion.SIMILARITY_128_BIT)
                    Logger.log("")
                    
            # fnv1 64 bit test
            for strVersion in index.ToStringVersion:
                Logger.log("\nRunning 64 bit %s fnv1 with %s tables:"%(strVersion.value, tableAmount))
                for rep in range(reps):
                    Logger.log("%s. measurement:"%(rep+1))
                    duplicatesSet = dedup.deduplicate(tableDict, strVersion, index.HashVersion.FNV1)
                    Logger.log("")
    
    return

def run_special_tests(conn, reps:int = 3, ranges:list = defaultBiggerRanges, corpusList:Set[db.CorpusType] = {db.CorpusType.GIT_TABLES, db.CorpusType.WEB_TABLES}):
    '''Executes a performance test of the faster algorithms developed in this thesis on different corpi.
    "conn": the connection this test uses to retrieve the data from.
    "reps": the amount of repretitions for each singular test.
    "ranges": the ranges of tables used to test faster algorithms.
    "corpusList":the different corpi used to test the faster algorithms.'''
     
    for corpusType in corpusList:
        Logger.log("Test on corpus %s:\n"%corpusType)
            
        for tableAmount in ranges:
            tableDict = db.retrieveTestDataIdRange(conn, corpusType, 0, tableAmount)
            
            Logger.log("\nRunning 128 bit simple simhash with %s tables:"%(tableAmount))
            for rep in range(reps):
                Logger.log("%s. measurement:"%(rep+1))
                duplicatesSet = dedup.deduplicate(tableDict, index.ToStringVersion.SIMPLE, index.HashVersion.SIMILARITY_128_BIT)
                Logger.log("")
    
            
            Logger.log("\nRunning 128 bit full simhash with %s tables:"%(tableAmount))
            for rep in range(reps):
                Logger.log("%s. measurement:"%(rep+1))
                duplicatesSet = dedup.deduplicate(tableDict, index.ToStringVersion.FULL, index.HashVersion.SIMILARITY_128_BIT)
                Logger.log("")
                
            Logger.log("\nRunning 64 bit simple fnv1 with %s tables:"%(tableAmount))
            for rep in range(reps):
                Logger.log("%s. measurement:"%(rep+1))
                duplicatesSet = dedup.deduplicate(tableDict, index.ToStringVersion.SIMPLE, index.HashVersion.FNV1)
                Logger.log("")
    
    return