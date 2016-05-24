from __future__ import print_function
import os
import unicodecsv
import urllib
from gevent.pool import Pool
import gevent.monkey
gevent.monkey.patch_socket()

item_fn = "data/data-20160516/item.txt"
mirror = "data/mirror"
source_url = "http://archive.org/download"

def download_file(file_spec):
    #try:
    resp = urllib.urlretrieve(file_spec["remote_fn"], file_spec["local_fn"])
    #except:
    #    print("Whoops!")

# read items as tsv
barcodes = []
with open(item_fn) as item_file:
    item_tsv = unicodecsv.DictReader(item_file, dialect="excel-tab")
    
    for item in item_tsv:
        #print(item)
        barcodes.append(item["BarCode"])
        #exit(0)
        
print ("Loaded {0} barcodes".format(len(barcodes)))

# create mirror directory if not found
if not os.path.isdir(mirror):
    os.mkdir(mirror)
    
# build list of things that need getting
file_suffixes = ["_djvu.txt", "_files.xml"]
file_specs = []
line = 1 # header row counts for one, this counter reflects the line in item.txt
for barcode in barcodes:
    line += 1
    for suffix in file_suffixes:
        try:
            local_fn = os.path.join(mirror, barcode) + suffix
            if not os.path.exists(local_fn):
                remote_fn = "/".join([source_url, barcode, barcode]) + suffix
                file_specs.append({"local_fn": local_fn,
                                   "remote_fn": remote_fn})
        except:
            print("Issue with barcode {0} on line {1}".format(barcode, line))

print("Need to fetch {0} files".format(len(file_specs)))

download_file(file_specs[0])

# iterate & download - make smart enough to do per-file in case we add file
# type later - wget-esque behavior, no state
    # if file does not exist...

    # _djvu.txt, _files.xml

    # log errors 
