from typing import Dict, List, Set
import pandas as pd
import sys
import psycopg2
from collections import defaultdict

from enum import Enum

import Logger


class CorpusType(Enum):
    WEB_TABLES = 'main_tokenized'
    GIT_TABLES = 'gittables_main_tokenized'
    MATE_MAIN = 'mate_main_tokenized'


collectMemory = True
memory = 0

loggerActive = False

#####################################################################################################################################################################################
# first version: retrieving test data as multidimensional dict without a superkey
# datastructure dimensions  index 1: tableid
#                           index 2: rowid
#                           index 3: colid
#                           value: token

def retrieveTestDataIdRange(connection, corpus: CorpusType, startID: int, endID: int) -> dict:
    """Retrieves all the tupels from tables of the given ID range from the given corpus of the given connection and stores them into a dict with a multidimensional index."""
    
    cursor = connection.cursor()
    cursor.execute(f'SELECT tableid, rowid, colid, tokenized FROM "{corpus.value}" WHERE tableid >= {startID} AND tableid <= {endID} ORDER BY tableid, rowid, colid;')
    results = cursor.fetchall()
    if(loggerActive): Logger.log("Retrieved Data successfully!")
    cursor.close()
    
    global memory
    if collectMemory:
        memory = memory + sys.getsizeof(results)
    
    # all the tokens for each cell are stored here 
    allTables = defaultdict(lambda: defaultdict(dict))
    
    # separate tupels by tables, rows and columns and store them in a structured manner
    for row in results:
        allTables[row[0]][row[1]][row[2]] = str(row[3])
    
    if(loggerActive): Logger.log("Structured Testdata!")
    return allTables

def retrieveTestDataIdSet(connection, corpus: CorpusType, ids: Set[int])->dict:
    """Retrieves all the tupels from tables of the given IDs from the given corpus of the given connection and stores them into a dict with a multidimensional index."""
    
    # prepare a string to use for the query
    idList = list(ids)
    idList.sort()
    idsString = str(idList)
    idsString = idsString.replace("[", "(")
    idsString = idsString.replace("]", ")")
    
    # fetch all the tables with the given ids
    cursor = connection.cursor()
    cursor.execute(f'SELECT tableid, rowid, colid, tokenized FROM {corpus.value} WHERE tableid in {idsString} ORDER BY tableid, rowid, colid;')
    results = cursor.fetchall()
    if(loggerActive): Logger.log("Retrieved Data successfully!")
    cursor.close()
    
    global memory
    if collectMemory:
        memory = memory + sys.getsizeof(results)
    
    # all the tokens for each cell are stored here 
    allTables = defaultdict(lambda: defaultdict(dict))
    
    # separate tupels by tables, rows and columns and store them in a structured manner
    for row in results:
        allTables[row[0]][row[1]][row[2]] = str(row[3])
    
    if(loggerActive): Logger.log("Structured Testdata!")
    return allTables

#####################################################################################################################################################################################
# second version: retrieving test data as multidimensional dict with a superkey per row
# datastructure dimensions  index 1: [0] = data     --      index 1: [1] = super_key
#                           index 2: rowid          --      index 2: rowid
#                           index 3: colid          --      value: super_key
#                           value: token
    

def retrieveTestDataWithSuperKeysIdRange(connection, startID: int, endID: int) -> dict:
    """Retrieves all the tupels from tables of the given ID range from the 'MATE_MAIN' corpus of the given connection and stores them into a dict with a multidimensional index.
    Super Keys are retrieved also, therefore the corpus is always 'MATE_MAIN'."""
    
    cursor = connection.cursor()
    cursor.execute(f'SELECT tableid, rowid, colid, tokenized, super_key FROM {CorpusType.MATE_MAIN.value} WHERE tableid >= {startID} AND tableid <= {endID} ORDER BY tableid, rowid, colid;')
    results = cursor.fetchall()
    if(loggerActive): Logger.log("Retrieved Data successfully!")
    cursor.close()
    
    global memory
    if collectMemory:
        memory = memory + sys.getsizeof(results)
    
    # all the tokens for each cell are stored here 
    allTablesData = defaultdict(lambda: defaultdict(dict))
    # all the Super Keys for each row are stored here
    allTablesSuperKey = defaultdict(lambda: defaultdict(dict))
    
    # separate tupels by tables, rows and columns and store them in a structured manner
    for row in results:
        
        allTablesData[row[0]][row[1]][row[2]] = str(row[3])
        allTablesSuperKey[row[0]][row[1]] = int(row[4],2) # convert to binary int
    
    Logger.log("Structured Testdata!")
    return [allTablesData, allTablesSuperKey]

def retrieveTestDataWithSuperKeysIdSet(connection, ids: Set[int])->dict:
    """Retrieves all the tupels from tables of the given IDs from the 'MATE_MAIN' corpus of the given connection and stores them into a dict with a multidimensional index.
    Super Keys are retrieved also, therefore the corpus is always 'MATE_MAIN'."""
    
    # prepare a string to use for the query
    idList = list(ids)
    idList.sort()
    idsString = str(idList)
    idsString = idsString.replace("[", "(")
    idsString = idsString.replace("]", ")")
    
    # fetch all the tables with the given ids
    cursor = connection.cursor()
    cursor.execute(f'SELECT tableid, rowid, colid, tokenized, super_key FROM {CorpusType.MATE_MAIN.value} WHERE tableid in {idsString} ORDER BY tableid, rowid, colid;')
    results = cursor.fetchall()
    if(loggerActive): Logger.log("Retrieved Data successfully!")
    cursor.close()
    
    global memory
    if collectMemory:
        memory = memory + sys.getsizeof(results)
    
    # all the tokens for each cell are stored here 
    allTablesData = defaultdict(lambda: defaultdict(dict))
    # all the Super Keys for each row are stored here
    allTablesSuperKey = defaultdict(lambda: defaultdict(dict))
    
    # separate tupels by tables, rows and columns and store them in a structured manner
    for row in results:
        
        allTablesData[row[0]][row[1]][row[2]] = str(row[3])
        allTablesSuperKey[row[0]][row[1]] = int(row[4],2) # convert to binary int
    
    if(loggerActive): Logger.log("Structured Testdata!")
    return [allTablesData, allTablesSuperKey]


