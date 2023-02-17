from collections import defaultdict
from enum import Enum
from typing import Callable, Dict, List

from simhash import Simhash



def preprocessData(allTables: defaultdict) -> defaultdict:
    '''Preprocesses the data by ordering all columns, and rows by a fixed order. 
    Removes duplicate rows also. Returns the result. 
    The input should be structured "[tableid][rowid][columnid]->data" 
    and will be returned in the same structure.'''
    
    tablesAsLists:Dict[int,List[list]] = defaultdict(dict)
    
    for tableid in allTables:
        table = allTables[tableid]
        
        rowsList = []
        
        for rowid in table:
            row = list(table[rowid].values())
            
            # attribute order is ignored when using a fixed sorting
            row.sort()
            rowsList.append(row)
            
        # no need to further process only one (or no) row
        if(len(rowsList) <= 1): 
            tablesAsLists[tableid] = rowsList
            continue    
        
        # row order is ignored also when using a fixed sorting 
        rowsList.sort()
        
        # remove duplicate rows
        rowsListWithoutDups = [rowsList[0]]
        for rowid in range(1, len(rowsList)):
            newRow = rowsList[rowid]
            
            # cheking only the latest added row works because of the sorting of the rows
            if(rowsListWithoutDups[-1] != newRow): # '-1' == last element
                rowsListWithoutDups.append(newRow)
            
        
        tablesAsLists[tableid] = rowsListWithoutDups
    
    # convert Dict[int,List[List[value]]] to Dict[[int][int][int],value]
    newTables = defaultdict(lambda: defaultdict(dict))
    
    for tableid in tablesAsLists:
        for rowid in range(len(tablesAsLists[tableid])):
            for columnid in range(len(tablesAsLists[tableid][rowid])):
                newTables[tableid][rowid][columnid] = tablesAsLists[tableid][rowid][columnid]
    
    return newTables

def toString_simple(table:List[list])->str:
    ret = ""
    
    for rowid in table:
        for colid in table[rowid]:
            ret += table[rowid][colid] + "\t"
        ret += "\n"
    
    return ret[:-2]

def toString_indent(table:List[list])->str:
    
    # empty table
    if len(table) == 0 or len(table[0]) == 0: return ""
    
    # find out how long the longest cell in each column is
    maxColLenght = [0]* len(table[0])
    
    for rowid in table:
        for colid in table[rowid]:
            cellLenght = len(table[rowid][colid])
            if(maxColLenght[colid] < cellLenght):
                maxColLenght[colid] = cellLenght
    
    ret = ""
    
    for rowid in table:
        for colid in table[rowid]:
            cellValue = table[rowid][colid]
            
            # find out how much padding is needed
            padLen = maxColLenght[colid] - len(cellValue)
            padding = " " * padLen
            
            ret += padding + cellValue + "  "
        ret += "\n"
    
    return ret[:-3]

def toString_full(table:List[list])->str:
    
    # empty table
    if len(table) == 0 or len(table[0]) == 0: return ""
    
    # find out how long the longest cell in each column is
    maxColLenMap = [0]* len(table[0])
    
    # also store the longest colid
    maxColIdLen = 0
    
    for rowid in table:
        for colid in table[rowid]:
            cellLenght = len(table[rowid][colid])
            if(maxColLenMap[colid] < cellLenght):
                maxColLenMap[colid] = cellLenght
                
            # it is also possible, that the string representation of the column is wider than the content of the column itself
            colidLen = len(str(colid))
            #if(maxColLenMap[colid] < colidLen):
            #    maxColLenMap[colid] = colidLen
            
            # also find the longest colid
            if maxColIdLen < colidLen:
                maxColIdLen = colidLen
    
    # find out how long the longest rowid is
    maxRowIdLen = len(str(len(table)-1))
    
    output = " " * (maxRowIdLen + 1)
    
    # first put the column descriptors in the first row of the output
    
    for colid in table[0]:
        # find out how much padding is needed
        colid_str = str(colid)
        padding_front = " " * (maxColLenMap[colid] - maxColIdLen + 1)
        padding_back = " " * (maxColIdLen - len(colid_str) + 1)
        
        output += padding_front + colid_str + padding_back
    output += "\n"
    
    # now put all the contents row by row
    
    for rowid in table:
        
        # find out how much padding is needed
        rowid_str = str(rowid)
        rowPadLen = maxRowIdLen - len(rowid_str)
        padding = " " * rowPadLen
        
        output += rowid_str + padding
            
        for colid in table[rowid]:
            cellValue = table[rowid][colid]
            
            # find out how much padding is needed
            rowPadLen = maxColLenMap[colid] - len(cellValue)
            padding = " " * (rowPadLen + 2)
            
            output += padding + cellValue
        output += "\n"
    
    return output[:-1]

class ToStringVersion(Enum):
    SIMPLE:str = "simple"
    INDENT:str = "indet"
    FULL:str = "full"
    
    def execute(self, table:List[list])->str:
        if(self == ToStringVersion.SIMPLE): return toString_simple(table)
        elif(self == ToStringVersion.INDENT): return toString_indent(table)
        elif(self == ToStringVersion.FULL): return toString_full(table)
        else: raise TypeError("The current type %s is not supported!"%self)
        
        
    
BITS:int = 128
def simhash(data:str, bits:int=BITS):
    '''Hashes the given string using the simhash algorithm.'''
    return Simhash(data, f=bits).value

FNV1_64_INIT = 0xcbf29ce484222325
FNV_64_PRIME = 0x100000001b3
def fnv1_64bit(data:str)->int:
    '''Hashes the given string using the 64 bit fnv1 algorithm.'''
    hash = FNV1_64_INIT
    for chr in data:
        # different weight for each char
        hash = (hash * FNV_64_PRIME)%(2**64)
        # convert char to byte and use XOR on the last 8 bits of the current hash
        hash = hash ^ ord(chr)
    
    return hash

class HashVersion(Enum):
    SIMILARITY_64_BIT:str = "simhash 64 bit"
    SIMILARITY_128_BIT:str = "simhash 128 bit"
    FNV1:str = "fnv1-hash"
    
    def execute(self, data:str)->int:
        if(self == HashVersion.SIMILARITY_64_BIT): return simhash(data, 64)
        elif(self == HashVersion.SIMILARITY_128_BIT): return simhash(data, 128)
        elif(self == HashVersion.FNV1): return fnv1_64bit(data)
        else: raise TypeError("The current type %s is not supported!"%self)
    
def create_exact(data:dict, strFunc:ToStringVersion=ToStringVersion.FULL, hashFunc:HashVersion=HashVersion.SIMILARITY_64_BIT)->Dict[int,list]:
    '''Creates and returns an index mapping of Simhash Buckets containing respective table IDs: simhash -> [tableIds...].'''
    
    # preprocessing to ensure perfect recall
    data = preprocessData(data)
    
    # buckets of hash values containing respective table ids
    hashMap:Dict[int:list] = dict()
    
    for tableId in data:

        #tableString = dataframes[tableId].to_string()

        # create hash of the table via a string representation
        tableString = strFunc.execute(data[tableId])
        hash = hashFunc.execute(tableString)
        
        # create a bucket for the simhash if not already present
        if hash not in hashMap:
            hashMap[hash] = []
            
        hashMap[hash].append(tableId)
    
    return hashMap

def create_fuzzy(data:dict, strFunc:ToStringVersion=ToStringVersion.FULL, alreadyPreprocessed:bool=False)->Dict[int,int]:
    '''Creates and returns an index mapping Simhashvalues to table IDs: tableId -> simhash.'''
     
    # preprocessing to ensure perfect recall
    if(not alreadyPreprocessed):
        data = preprocessData(data)
    
    # buckets of hash values containing respective table ids
    hashMap:Dict[int:list] = dict()
    
    for tableId in data:
        
        # create hash of the table via a string representation
        tableString = strFunc.execute(data[tableId])
        hash = HashVersion.SIMILARITY_128_BIT.execute(tableString)
        
        # mapping hashes onto table IDs
        hashMap[tableId] = hash
    
    return hashMap
    