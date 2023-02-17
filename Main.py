import sys
import psycopg2

import deduplicators.hash_xash_deduplicator as hxdd
import deduplicators.deduplicator as dedup
from deduplicators.deduplicator import SimilarityFunction
import time_handler
from db_handler import *
from utils import *
from index import *
import Logger
import performance_test
import index

def main():
    
    # important, since some tables contain weird characters
    sys.stdout.reconfigure(encoding='utf-8')
    Logger.clear()
    
    # Connect to an existing database
    connection = psycopg2.connect(host = "", port = "", dbname="", user="", password= "")
    
    # all the Exact Contents Experiments
    
    #performance_test.run_default_tests(connection, reps=3, slowerAlgRanges=[10000, 20000, 30000, 40000, 50000], fasterAlgRanges=[10000, 20000, 30000, 40000, 50000, 60000, 70000])
    #performance_test.run_special_tests(connection, reps=3, ranges=[10000, 20000, 30000, 40000, 50000])
    
    startID =   1
    endID =     5000
    offset = 0
    
    # data investigations
    tableDict = retrieveTestDataIdRange(connection, CorpusType.GIT_TABLES, startID+offset, endID+offset)

    '''
    tableCount = 0
    rowSum = 0
    columnSum = 0

    for tableid in tableDict:
        tableData = tableDict[tableid]
        
        # empty table
        if(len(tableData) == 0): continue
        
        tableCount += 1
        rowSum += len(tableData)
        columnSum += len(tableData[0])
        
    Logger.log("Tablecount: %s, average row count: %s, average column count: %s"%(tableCount, rowSum/tableCount, columnSum/tableCount))
    #'''
    
    
    duplicateGroups = dedup.deduplicate(data=tableDict, strFunc=ToStringVersion.SIMPLE, hash=HashVersion.FNV1)
    
    '''
    tableCount = 0
    rowSum = 0
    columnSum = 0
    for group in duplicateGroups:
        for tableid in group:
            tableData = tableDict[tableid]
            
            # empty table
            if(len(tableData) == 0): continue
            
            tableCount += 1
            rowSum += len(tableData)
            columnSum += len(tableData[0])
        
    Logger.log("Tablecount: %s, average row count: %s, average column count: %s"%(tableCount, rowSum/tableCount, columnSum/tableCount))
    #'''
    
    '''
    fpIDList:List[tuple] = dedup.tempFPList
    
    Logger.log("FP pairs: %s"%fpIDList)

    pairCount = 0
    rowSum = 0
    columnSum = 0
    for fpIDPair in fpIDList:
        tableData1 = tableDict[fpIDPair[0]]
        tableData2 = tableDict[fpIDPair[1]]
        
        # empty table
        if(len(tableData1) == 0 or len(tableData2) == 0): continue
        pairCount += 1
        rowSum += (len(tableData1) + len(tableData2))/2.0
        columnSum += (len(tableData1[0]) + len(tableData2[0]))/2.0

    Logger.log("Paircount: %s, average row count: %s, average column count: %s"%(pairCount, rowSum/pairCount, columnSum/pairCount))
    #'''


    # Similarity Evaluation Experiment + Similarity Degree Experiment

    '''
    tresholds = [1, 0.99, 0.95, 0.90, 0.85, 0.80, 0.75, 0.70, 0.65, 0.60, 0.55, 0.50, 0.45, 0.40, 0.35, 0.30, 0.25, 0.20, 0.15, 0.10, 0.05, 0]
        
    for treshold in tresholds:
        
        duplicatesSetFuzzy = dedup.deduplicate_fuzzy(tableDict, ToStringVersion.FULL, 3, SimilarityFunction.LEVENSHTEIN_SIMILARITY, treshold, experimental=False)
        Logger.log("Fuzzy: %s"%duplicatesSetFuzzy)
        
        
        onlyFuzzySet = remove_subsets(sym_diff_list_unhashable(duplicatesSet, duplicatesSetFuzzy))
        Logger.log("Difference: %s\n"%onlyFuzzySet)
    
    #{3465, 3466}
    
    #'''
    
    #printAllTablesWithIDs({59, 60}, connection, CorpusType.MATE_MAIN)
    
    # Close communication with the database
    connection.close()
    
if __name__ == '__main__':
    main()