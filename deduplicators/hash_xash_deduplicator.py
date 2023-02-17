import utils

from collections import defaultdict
import copy
from typing import Set
import Logger
import time_handler

# is filled up to stores a mapping of columns for potentially each row of each table
attributeMapping_cache = defaultdict(dict)
# stores the amount of times a value appears for every row
attributeValueCount_cache = defaultdict(dict)

def __compareTables(t1_id : int, t2_id: int, data: dict, duplicateRowsAllowed: bool) -> bool:
    '''Compares the to given tables and returns whether they are equal'''
    
    t1_superkeys = data[1][t1_id]
    t2_superkeys = data[1][t2_id]
    
    # compare amount of rows if two tables with different amounts of rows are not considered duplicate
    if(not duplicateRowsAllowed and len(t1_superkeys) != len(t2_superkeys)): return False
    
    # empty tables
    if(len(t1_superkeys) == 0 or len(t2_superkeys) == 0): return False
    
    # figures out which tables has less tuples
    # this is an optimization
    if(len(t1_superkeys) > len(t2_superkeys)):
        bigger_table_superkeys = t1_superkeys
        bigger_table_id = t1_id
        smaller_table_superkeys = t2_superkeys
        smaller_table_id = t2_id
    else:
        bigger_table_superkeys = t2_superkeys
        bigger_table_id = t2_id
        smaller_table_superkeys = t1_superkeys
        smaller_table_id = t1_id
    
    # used for making sure the attributes of the different tables are mapped properly
    column_mapping = dict()
    global attributeMapping_cache
    hashjoinMap = dict()
    global attributeValueCount_cache 
    
    # creates a hash index: superkey -> [table_ids...]
    for bRowId in bigger_table_superkeys:
        superkey_big = bigger_table_superkeys[bRowId]
        # add an entry for the superkey if the superkey is not already present
        if superkey_big not in hashjoinMap:
            hashjoinMap[superkey_big] = []
        hashjoinMap[superkey_big].append(bRowId)
        
    
    
    
    # stores how many matches have been found in total
    dupRowCount = 0
    # stores all the ids of rows of the smaller table, that have found mathing rows in the other table
    sRowMatchSet = set()
    # stores all the ids of rows of the bigger table, that have found mathing rows in the other table
    bRowMatchSet = set()
    
    
    
    # now find out if every row of the smaller table has at least one...
    # ...corresponding row in the other table
    for sRowId in smaller_table_superkeys:
        
            superkey_small = smaller_table_superkeys[sRowId]
            
            # every row of the smaller table needs to exist at least once in the bigger table
            # if there is not even a bucket for the hashvalue of this row,...
            # ...that means the other table does not have a duplicate row for this sRow
            if superkey_small not in hashjoinMap:
                return False
            
            sRow =  list(data[0][smaller_table_id][sRowId].values())
            
            # find or generate the attribute mapping for this row
            if(sRowId not in attributeMapping_cache[smaller_table_id]):
                # generate and store
                smaller_attributeMapping = dict()
                for i in range(len(sRow)):
                    if(sRow[i] not in smaller_attributeMapping):
                        smaller_attributeMapping[sRow[i]] = []
                    smaller_attributeMapping[sRow[i]].append(i)
                attributeMapping_cache[smaller_table_id][sRowId] = smaller_attributeMapping
            else: 
                # use existing
                smaller_attributeMapping = attributeMapping_cache[smaller_table_id][sRowId]
            
            # find or generate a map storing the amount of times any value is present in the given row
            if sRowId not in attributeValueCount_cache[smaller_table_id]:
                # generate and store
                attributeCountMap = {}
                for cellValue in sRow:
                    if cellValue in attributeCountMap:
                        attributeCountMap[cellValue] += 1
                    else:
                        attributeCountMap[cellValue] = 1
                attributeValueCount_cache[smaller_table_id][sRowId] = attributeCountMap
            else:
                # use existing
                attributeCountMap = attributeValueCount_cache[smaller_table_id][sRowId]
                
            # now compare the sRow to every row of the bigger table in the same bucket as this sRow
            # this loop does not stop after finding one duplicate row,...
            # ...so all the rows of the other table find their duplicate as well
            for bRowId in hashjoinMap[superkey_small]: 
                
                bRow = list(data[0][bigger_table_id][bRowId].values())
                
                # empty rows cannot be equal to sRow TODO check, because this sRow may be emtpy
                if(len(bRow) <= 0): continue
                
                # the rows of the bigger table also need an attribute mapping
                if(bRowId not in attributeMapping_cache[bigger_table_id]):
                    # generate and store
                    bigger_attributeMapping = dict()
                    for i in range(len(bRow)):
                        if(bRow[i] not in bigger_attributeMapping):
                            bigger_attributeMapping[bRow[i]] = []
                        bigger_attributeMapping[bRow[i]].append(i)
                    attributeMapping_cache[bigger_table_id][bRowId] = bigger_attributeMapping
                else: 
                    # use existing
                    bigger_attributeMapping = attributeMapping_cache[bigger_table_id][bRowId]
                    
                # this algorithm uses not the same mapping as for the bigger table
                # instead of finding matches for lists of all indexes of the attributes,...
                # ...only the index of the latest occurence is matched to a list of possible attributes...
                # ...from the smaller table
                bigger_attributeMapping ={k: bigger_attributeMapping.get(k)[-1] for k in bigger_attributeMapping}    
                    
                count_copy = copy.deepcopy(attributeCountMap)
                mapping_copy = copy.deepcopy(column_mapping) # FIX 1: do not commit change until after the row is proven duplicate
                
                # flags if this row comparison failed and the next one should be considered
                fail = False
                
                # analogous to a cell per cell comparison
                for cellValue in bRow:
                    
                    # check if the current cell value of the bRow even exists enough times in the smaller table still
                    if cellValue not in count_copy or count_copy[cellValue] == 0: 
                        fail = True
                        break
                    
                    # dissallow multiple cells with the same value to connect to a single cell...
                    # ...from the other table multiple times
                    count_copy[cellValue] -= 1
                    
                    # make sure there is a proper 1-to-m attribute mapping, whereas m is the amount of colums,...
                    # ... that could still map to the column of the attribute of the other table
                    if bigger_attributeMapping[cellValue] not in mapping_copy:
                        # every pair of matching cell values enforces a mapping of attributes for the entire table,
                        # (but only if the row turns out to be a duplicate!)
                        mapping_copy[bigger_attributeMapping[cellValue]] = smaller_attributeMapping[cellValue]
                    else:
                        # in case there was already an attribute mapping stored for the two tables,...
                        # ...this row has to abide to that mapping. Otherwise the two rows cannot be duplicates
                        # the intersection takes out all the false mappings determined with the new information from the new pairing
                        mapping_copy[bigger_attributeMapping[cellValue]] = utils.intersect_list(mapping_copy[bigger_attributeMapping[cellValue]], smaller_attributeMapping[cellValue]) # FIX 2
                        if len(mapping_copy[bigger_attributeMapping[cellValue]]) > 0:
                            continue
                        else:
                            fail = True
                            break
                        
                if not fail: 
                    # found a duplicate row
                    dupRowCount += 1
                    sRowMatchSet.add(sRowId)
                    bRowMatchSet.add(bRowId)
                    # only accept mappings of actual duplicate rows
                    column_mapping = mapping_copy
          
    # make sure there are enough duplicates so that it is possible all of the rows of at least one table...
    # ...have found duplicate rows in the other table
    minNumberRows = min(len(t1_superkeys), len(t2_superkeys))
    if dupRowCount < minNumberRows or minNumberRows <= 0 : return False
    
    # all the rows of at least one of the two tables have to have a matching row in the other table 
    if(len(sRowMatchSet) < len(smaller_table_superkeys) or len(bRowMatchSet) < len(bigger_table_superkeys)):
        return False
                                
    # finnaly, only if all checks were passed, the duplicate is detected
    return True

def deduplicate(data: dict, duplicateRowsAllowed:bool = True) -> list:
    '''Finds duplicate tables using hashtables and groups the ids of those duplicate tables together. 
    All the sets of duplicates are returned as a list. This function determines duplicates of the type 2+3+4.
    Duplicates of the type 4 can be turned off by setting 'duplicateRowsAllowed' to False.'''
    
    Logger.log("starting Hash-XASH Deduplicator...")
    
    with time_handler.measure_time("Hash-XASH"):
    
        # no tables
        if(len(data[0]) == 0):
            Logger.log("Empty Input!")
            return []
        
        # reset global caches
        global attributeMapping_cache
        attributeMapping_cache = defaultdict(dict)
        global attributeValueCount_cache
        attributeValueCount_cache = defaultdict(dict)
        
        
        Logger.log("creating buckets...", end="")
        
        # group tables into buckets by number of cols
        # this reduces necessary comparisons
        tablesBuckets: Set[Set[int]] = {}
        for tableId in data[0]:
            columnNumber = len(data[0][tableId][0])
            # Check if key exists and append a group for it otherwise
            if columnNumber not in tablesBuckets:
                tablesBuckets[columnNumber] = []
                
            tablesBuckets[columnNumber].append(tableId)

        Logger.log("done!\nfinding duplicates...", end="")

        duplicatesGroups = []

        # compare every table with every other table...
        for num_cols, tableIds in tablesBuckets.items():
            for tableIdt1 in tableIds:
                for tableIdt2 in tableIds:
                    # ...but only in one direction (that is to say not both t1 <=> t2 and t2 <=> t1)
                    if tableIdt1 < tableIdt2:
                        # check whether the two tables are duplicates
                        # in case they are duplicates one of 2 cases can happen:
                        #   1) there is no goup for the duplicates yet and it has to be created
                        #   2) only the higher value id needs to be added to the goup of the lower value id, since all the ids are traversed in natural number order
                        if(__compareTables(tableIdt1, tableIdt2, data, duplicateRowsAllowed)):
                            # find the group to put the duplicates into (if present)
                            fittingGroupPresent = False
                            for group in duplicatesGroups:
                                if(tableIdt1 in group):
                                    fittingGroupPresent = True
                                    group.add(tableIdt2)
                                    break
                            # add a new set of duplicates if these duplicates do not have a group yet
                            if(not fittingGroupPresent):
                                duplicatesGroups.append({tableIdt1, tableIdt2})
        
        Logger.log("done!")
                            
    return duplicatesGroups