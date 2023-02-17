from collections import defaultdict
import copy
from enum import Enum
import rapidfuzz

from typing import Callable, Dict, Set
from index import HashVersion, ToStringVersion
import time_handler
import utils
import index
import Logger

######################################################################################################################################################
# Exact Contents

# is filled up to stores a mapping of columns for potentially each row of each table
attributeMapping_cache = defaultdict(dict)
# stores the amount of times a value appears for every row
attributeValueCount_cache = defaultdict(dict)

def __compareTables(t1_id : int, t2_id: int, data: dict) -> bool:
    '''Compares the to given tables and returns whether they are duplicates of the type 2+3+4'''
    
    t1_data = data[t1_id]
    t2_data = data[t2_id]
    
    # empty tables
    if(len(t1_data) == 0 or len(t2_data) == 0): return False
    
    # compare number of columns
    if(len(t1_data[0]) != len(t2_data[0])): return False
    
    # figures out which tables has less tuples
    # this is an optimization
    if(len(t1_data) > len(t2_data)):
        bigger_table_data = t1_data
        bigger_table_id = t1_id
        smaller_table_data = t2_data
        smaller_table_id = t2_id
    else:
        bigger_table_data = t2_data
        bigger_table_id = t2_id
        smaller_table_data = t1_data
        smaller_table_id = t1_id
    
    # used for making sure the attributes of the different tables are mapped properly
    column_mapping = dict()
    global attributeMapping_cache
    global attributeValueCount_cache 
    
    # stores how many matches have been found in total
    dupRowCount = 0
    # stores all the ids of rows of the smaller table, that have found mathing rows in the other table
    sRowMatchSet = set()
    # stores all the ids of rows of the bigger table, that have found mathing rows in the other table
    bRowMatchSet = set()
    
    # now find out if every row of the smaller table has at least one...
    # ...corresponding row in the other table
    for sRowId in smaller_table_data:
        sRow =  list(data[smaller_table_id][sRowId].values())
    
         # find or generate the attribute mapping for this row
        if(sRowId not in attributeMapping_cache[smaller_table_id]):
            # generate and store
            smaller_attributeMapping:Dict[int, list] = dict()
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
            
        # now compare the sRow to every row of the bigger table
        # this loop does not stop after finding one duplicate row,...
        # ...so all the rows of the other table find their duplicate as well
        for bRowId in bigger_table_data:
            bRow = list(data[bigger_table_id][bRowId].values())
            
            # empty rows cannot be equal to sRow TODO check, because this sRow may be emtpy
            if(len(bRow) <= 0): continue
            
            # the rows of the bigger table also need an attribute mapping
            if(bRowId not in attributeMapping_cache[bigger_table_id]):
                # generate and store
                bigger_attributeMapping:Dict[int, list] = dict() # see later why 'list' and not 'set'
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
            bigger_attributeMapping ={k: bigger_attributeMapping.get(k)[-1] for k in bigger_attributeMapping} # [-1] is why 'list' ordering is important (always draws deterministic values)
            
            count_copy = copy.deepcopy(attributeCountMap)
            mapping_copy = copy.deepcopy(column_mapping)
            
            # flags if this row comparison failed and the next one should be considered
            fail = False
            
            # cell per cell comparison
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
                    mapping_copy[bigger_attributeMapping[cellValue]] = utils.intersect_list(mapping_copy[bigger_attributeMapping[cellValue]], smaller_attributeMapping[cellValue])
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
    minNumberRows = min(len(t1_data), len(t2_data))
    if dupRowCount < minNumberRows or minNumberRows <= 0 : return False
    
    # all the rows of at least one of the two tables have to have a matching row in the other table 
    if(len(sRowMatchSet) < len(smaller_table_data) or len(bRowMatchSet) < len(bigger_table_data)):return False
    
    return True

# precision calculation
fpCount = 0
tpCount = 0

tempFPList = []

def deduplicate(data: dict, strFunc:ToStringVersion=ToStringVersion.FULL, hash:HashVersion=HashVersion.SIMILARITY_64_BIT) -> list:
    '''Finds duplicate tables and groups the ids of those duplicate tables together.
    All the sets of duplicates are returned as a list. This function determines duplicates of the type 2+3+4.'''
    
    global tempFPList
    tempFPList.clear()
    
    versionString = "%s %s"%(strFunc.value, hash.value)
    
    Logger.log("starting %s..."%(versionString))
    
    with time_handler.measure_time(versionString):
    
        # no tables
        if(len(data) == 0):
            Logger.log("Empty Input!")
            return []
        
        # reset
        global attributeMapping_cache
        global attributeValueCount_cache
        attributeMapping_cache = defaultdict(dict)
        attributeValueCount_cache = defaultdict(dict) # TODO PUSH CHANGES!!
        
        global fpCount
        global tpCount
        fpCount = 0
        tpCount = 0
        
        with time_handler.measure_time("Hash creation"):
            Logger.log("creating buckets and hashvalues...", end="")
            hashMap = index.create_exact(data, strFunc, hash)
            Logger.log("done!")

        with time_handler.measure_time("Deduplication"):
            Logger.log("finding duplicates...", end="")

            duplicatesGroups = []
            duplicatesBuckets = dict()
        
            # compare every table with every other table in the same bucket...
            for bucket in hashMap:
                #if(len(simhashMap.get(bucket)) > 1):
                #    Logger.log("Bucket ID %s: %s"%(bucket, simhashMap.get(bucket)))
                for tableIdt1 in hashMap[bucket]:
                    for tableIdt2 in hashMap[bucket]:
                        # ...but only in one direction (that is to say not both t1 <=> t2 and t2 <=> t1)
                        if tableIdt1 < tableIdt2:
                            #Logger.log("Comparing %s and %s:"%(tableIdt1, tableIdt2))
                            # check whether the two tables are duplicates
                            if(__compareTables(tableIdt1, tableIdt2, data)):
                                if(bucket not in duplicatesBuckets): duplicatesBuckets[bucket] = []
                                duplicatesBuckets[bucket].append((tableIdt1, tableIdt2))
                                tpCount += 1
                            else:
                                tempFPList.append((tableIdt1, tableIdt2))
                                fpCount +=1

            Logger.log("done!")
            
        with time_handler.measure_time("Grouping"):
            Logger.log("collecting duplicate pairs in groups...", end="")
            
            duplicatesGroups = []
            
            for bucket in duplicatesBuckets:
                duplicatesList = duplicatesBuckets[bucket]
                
                for tablePair in duplicatesList:
                    tableIdt1 = tablePair[0]
                    tableIdt2 = tablePair[1]
                    # there are only two relevant cases for pairs of duplicates in this algorithm:
                    #   1) there is no group for the current pair of duplicates duplicates yet and the group has to be created
                    #   2) only the higher value id needs to be added to the group of the lower value id,...
                    #       ...since all the ids are traversed in natural number order and thus the lower value id is already present in the group
                    
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
        
    
    precision = 0 if tpCount <= 0 else round((tpCount/(tpCount+fpCount))*100000)/1000
    Logger.log("Found %s FP's, %s TP's -> precision = %s!"%(fpCount,tpCount, precision))
                            
    return duplicatesGroups




######################################################################################################################################################
# Similar Contents

def jaccard_index(a:str, b:str)->float:
    '''calculates the jaccard index of the two given strings (with set logic).'''
    unionLen = len(utils.union_string(a,b))
    if(unionLen == 0): return 0
    return len(utils.intersect_string(a,b))/unionLen

def levenshtein_similarity(a:str, b:str)->float:
    '''Calculates a normalized similarity score based on the levenshtein distance, also known as edit distance, of the two given strings.'''
    
    # special case: two empty strings
    maxLen = max(len(a), len(b))
    if(maxLen == 0): return 1
    
    editDistance = rapidfuzz.distance.Levenshtein.distance(a,b)
    similarity = 1.0 - (editDistance / maxLen) # maxLen != 0
    return similarity

class SimilarityFunction(Enum):
    JACCARD_INDEX:str="Jaccard Index"
    LEVENSHTEIN_SIMILARITY:str="Levenshtein Similarity"

    def execute(self, string_a:str, string_b:str)->float:
        if(self == SimilarityFunction.JACCARD_INDEX): return jaccard_index(string_a, string_b)
        elif(self == SimilarityFunction.LEVENSHTEIN_SIMILARITY): return levenshtein_similarity(string_a, string_b)
        else: raise TypeError("The current type %s is not supported!"%self)
    
    
def calculateSimilarityScore(t1_id : int, t2_id: int, data: dict, simFunc:SimilarityFunction)->float:
    '''Calculates a similarity score for two tables with the given IDs from the given data 
        using the given similarity function.'''
    
    t1_data = data[t1_id]
    t2_data = data[t2_id]
    
    # empty tables
    if(len(t1_data) == 0 or len(t2_data) == 0): return 0
    
    # compare number of columns
    if(len(t1_data[0]) != len(t2_data[0])): return 0
    
    # compare number of row
    # since this implementation uses preprocessed data,...
    # ...differences in the amount of rows means the smaller table is missing rows with specific content the other table has
    if(len(t1_data) != len(t2_data)): return 0
    
    # calculates a similarity score for each pair of cells using a 1 to 1 mapping
    # this works since the tables are preprocessed and have the same amount of rows/columns
    scores = []
    for rowId in t1_data: 
        for colId in t1_data[rowId]:
            t1_cell = t1_data[rowId][colId]
            t2_cell = t2_data[rowId][colId]
            score = simFunc.execute(t1_cell, t2_cell)
            if(score != 1): print("'%s' and\n'%s'"%(t1_cell, t2_cell))
            scores.append(score)
    
    # average the scores to determine an overall score for the similarity of the two tables
    averageScore = sum(scores)/len(scores)
    return averageScore

def calculateSimilarityScore_experimental(t1_id : int, t2_id: int, data: dict, simFunc:SimilarityFunction)->float:
    '''Calculates a similarity score for two tables with the given IDs from the given data 
        using the given similarity function. This is an experimental version and not the one used in the experiments.
        This function does not match cell based on content similarity appropriately yet. 
        A proper match function would problably improve the results a lot. But it might also increase execution time.'''
    
    t1_data = data[t1_id]
    t2_data = data[t2_id]
    
    # empty tables
    if(len(t1_data) == 0 or len(t2_data) == 0): return 0
    
    # calculates a similarity score for each pair of cells using a 1 to 1 mapping
    # this works since the tables are preprocessed and have the same amount of rows/columns
    scores = []
    maxRowID = max(len(t1_data), len(t2_data))-1
    maxColID = max(len(t1_data[0]), len(t2_data[0]))-1
    for rowId in range(maxRowID+1): 
        for colId in range(maxColID+1):
            
            # make sure the cell is present in t1 and t2
            if(len(t1_data)-1 < maxRowID or len(t2_data)-1 < maxRowID
               or len(t1_data[0])-1 < maxColID or len(t2_data[0])-1 < maxColID):
                scores.append(0) # cells do not match at all, since there is no patner
                continue
            
            t1_cell = t1_data[rowId][colId]
            t2_cell = t2_data[rowId][colId]
            score = simFunc.execute(t1_cell, t2_cell)
            if(score != 1): print("'%s' and\n'%s'"%(t1_cell, t2_cell))
            scores.append(score)
    
    # average the scores to determine an overall score for the similarity of the two tables
    averageScore = sum(scores)/len(scores)
    return averageScore
    
def deduplicate_fuzzy(data: dict, strFunc:ToStringVersion=ToStringVersion.FULL, K:int=3, simFunc:SimilarityFunction=SimilarityFunction.LEVENSHTEIN_SIMILARITY, simThreshold:float = 0.9, experimental:bool=False) -> list:
    '''Finds duplicate tables and groups the ids of those duplicate tables together.
    All the sets of duplicates are returned as a list. This function determines duplicates of the type 2+3+4+5.
    Only uses Simhash 128 Bit.'''
    
    if(simThreshold < 0 or simThreshold > 1): 
        raise AttributeError("The given threshold %s is not between 0 and 1 (inclusive)!"%simThreshold)
    
    # always use simhash for similar contents
    hash = HashVersion.SIMILARITY_128_BIT
    
    versionString = "fuzzy %s %s with treshold %s"%(strFunc.value, hash.value, simThreshold)
    Logger.log("starting %s..."%(versionString))
    
    global tempFPList
    tempFPList.clear()
    
    with time_handler.measure_time(versionString):
    
        # no tables
        if(len(data) == 0):
            Logger.log("Empty Input!")
            return []
        
        # reset
        global fpCount
        global tpCount
        fpCount = 0
        tpCount = 0
        
        with time_handler.measure_time("Hash creation"):
            Logger.log("creating buckets and hashvalues...", end="")
            
            # the data is preprocessed here, since it is only needed preprocessed later on
            data = index.preprocessData(data)
            hashMap = index.create_fuzzy(data, strFunc, True)
            Logger.log("done!")
            
        with time_handler.measure_time("Deduplication"):
            Logger.log("finding duplicates...", end="")

            duplicatesPairs = []
        
            # compare every table with every other table, sadly there is no bucket optimization here
            for tableIdt1 in data:
                for tableIdt2 in data:
                    # ...but only in one direction (that is to say not both t1 <=> t2 and t2 <=> t1)
                    if tableIdt1 < tableIdt2:
                        
                        # simhash filter 
                        bitDifference = bin(hashMap[tableIdt1] ^ hashMap[tableIdt2]).count("1")
                        if(bitDifference > K): continue
                        
                        score = 0
                        if(experimental): score = calculateSimilarityScore_experimental(tableIdt1, tableIdt2, data, simFunc)
                        else: score = calculateSimilarityScore(tableIdt1, tableIdt2, data, simFunc)
                        
                        # check whether the two tables are duplicates using similarity
                        if(score >= simThreshold):
                            duplicatesPairs.append((tableIdt1, tableIdt2))
                            tpCount += 1
                        else:
                            tempFPList.append((tableIdt1, tableIdt2))
                            fpCount +=1
        
            Logger.log("done!")
    
        with time_handler.measure_time("Grouping"):
            Logger.log("collecting duplicate pairs in groups...", end="")
            
            duplicatesGroups = []
            

            for tablePair in duplicatesPairs:
                tableIdt1 = tablePair[0]
                tableIdt2 = tablePair[1]
                # there are only two relevant cases for pairs of duplicates in this algorithm:
                #   1) there is no group for the current pair of duplicates duplicates yet and the group has to be created
                #   2) only the higher value id needs to be added to the group of the lower value id,...
                #       ...since all the ids are traversed in natural number order and thus the lower value id is already present in the group
                
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
        
    
    precision = 0 if tpCount <= 0 else round((tpCount/(tpCount+fpCount))*100000)/1000
    Logger.log("Found %s FP's, %s TP's -> precision = %s!"%(fpCount,tpCount, precision))
    
    return duplicatesGroups