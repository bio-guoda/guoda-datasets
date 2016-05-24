from __future__ import print_function
import unicodecsv

item_fn = "data/data-20160516/item.txt"


# read items as tsv
items = []
with open(item_fn) as item_file:
    item_tsv = unicodecsv.DictReader(item_file, dialect="excel-tab")
    
    for item in item_tsv:
        #print(item)
        items.append(item)
        #exit(0)
        
print (len(items))

# verify barcode column comes through

# iterate & download - make smart enough to do per-file in case we add file
# type later - wget-esque behavior, no state
    # if file does not exist...

    # _djvu.txt, _files.xml

    # log errors 
