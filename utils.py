from collections import defaultdict
from typing import Dict, List

import pandas as pd
import psycopg2

from db_handler import *


def createDataFramesDict(allTables: defaultdict) -> Dict[int,pd.DataFrame]:
    '''Creates a DataFrame for each table in the given dict. Stores them into a dict mapping the tableid onto the DataFrame'''
    
    allDataFrames = dict()
    
    # overwrites the original table dicts column lists to store the tokens directly
    for tableid in allTables:
        table = allTables[tableid]
        rowList = []

        # add all cellvalues of one row as a list to the row
        for i in table:
            row = []
            
            for j in table[i]:
                row.append(table[i][j])
            
            rowList.append(row)
            #print(columnDict)

        allDataFrames[tableid] = pd.DataFrame(rowList)
    
    return allDataFrames

def printAllTables(allTables: defaultdict):
    allDataFrames: Dict[int, pd.DataFrame] = createDataFramesDict(allTables)
    
    # print out all the tables nicely using the pd.DataFrame strings
    for tableid in allDataFrames:
            print("Table ID: %s:\n%s\n"%(tableid, allDataFrames[tableid].to_string()))

def printAllTablesWithIDs(ids: set, connection: psycopg2.connect, corpus: CorpusType,) -> None:
    """Prints all the tables with the given IDs from the given corpus of the given connection."""
    
    allTables = retrieveTestDataIdSet(connection, corpus, ids)
    printAllTables(allTables)

def logAllTables(allTables: defaultdict, filename="output.txt"):
    allDataFrames: Dict[int, pd.DataFrame] = createDataFramesDict(allTables)
    
    # print out all the tables nicely using the pd.DataFrame strings
    for tableid in allDataFrames:
            msg = "Table ID: %s:\n%s\n"%(tableid, allDataFrames[tableid].to_string())
            Logger.log(msg, filename=filename)

def createTableDataWithoutSuperkey(data:List[List[list]])->dict:
    
    tables = defaultdict(lambda: defaultdict(dict))
    
    for tableId in range(len(data)):
        table = data[tableId]
        for rowId in range(len(table)):
            row = table[rowId]
            for columnId in range(len(row)):
                value = row[columnId]
                # only one table with id 0
                tables[tableId][rowId][columnId] = value
    
    return tables

def createTableDataWithSuperkeys(data:List[List[list]], superkeys:List[list])->dict:
    
    tables = defaultdict(lambda: defaultdict(dict))
    keys = defaultdict(lambda: defaultdict(dict))
    
    for tableId in range(len(data)):
        table = data[tableId]
        for rowId in range(len(table)):
            row = table[rowId]
            keys[tableId][rowId] = superkeys[tableId][rowId]
            for columnId in range(len(row)):
                value = row[columnId]
                # only one table with id 0
                tables[tableId][rowId][columnId] = value
                
    return [tables, keys]

def intersect_set(a:set, b:set)->set:
    '''Intersects the two given sets with set logic, 
    that is to say every value appears at most 1 time in the resulting set.'''
    return set.intersection(a, b)

def intersect_list(a:list, b:list)->list:
    '''Intersects the two given lists with set logic, 
    that is to say every value appears at most 1 time in the resulting list.'''
    return list(set.intersection(set(a), set(b)))

def sym_diff_list_hashable(a:list, b:list)->list:
    '''Returns symmetric difference of both lists using set logic,
    that is to say every value appears at most 1 time in the resulting list.
    This function only accepts hashable contents in the input list contents.
    That excludes "set"s for example.'''
    return set.symmetric_difference(set(a), set(b))

def sym_diff_list_unhashable(a:list, b:list)->list:
    '''Returns symmetric difference of both lists using set logic,
    that is to say every value appears at most 1 time in the resulting list.
    This function also accepts non-hashable contents in the input list contents.'''
    result = []
    
    b:list = b.copy()
    
    for content in a:
        if(content in b): b.remove(content)
        else: result.append(content)
    
    result.extend(b)
    return result

def remove_subsets(l:list)->list:
    '''Returns a list of sets, where each set is the biggest variant present.
    The input list needs to be a list of sets.'''
    result = l.copy()
    
    backToWhileLoop = True
    
    while(len(result) != 0 and backToWhileLoop):
        
        backToWhileLoop = False

        for i in range(len(result)):
            for j in range(i+1, len(result)):
                set1:set = result[i]
                set2:set = result[j]
                
                if(set1 == set2):
                    result.pop(j)
                    backToWhileLoop = True
                    break
                
                if(len(set1) < len(set2) and set1.issubset(set2)):
                    result.pop(i)
                    backToWhileLoop = True
                    break
                
                elif(len(set1) > len(set2) and set2.issubset(set1)):
                    result.pop(j)
                    backToWhileLoop = True
                    break                    
            
            # break to while loop
            if(backToWhileLoop): break
           
    return result

def intersect_string(a:str, b:str)->set:
    '''Intersects of the two given strings with set logic,
    that is to say every char appears at most 1 time in the resulting set.'''
    return set.intersection(set(a), set(b))
    
def union_string(a:str, b:str)->set:
    '''Caculates the union of the two given strings with set logic,
    that is to say every char appears at most 1 time in the resulting set.'''
    return set.union(set(a), set(b))




