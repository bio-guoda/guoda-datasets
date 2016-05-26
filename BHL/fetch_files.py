from __future__ import print_function
import os
import unicodecsv
import urllib
import time
import logging
from gevent.pool import Pool
import gevent.monkey
gevent.monkey.patch_socket()

def download_file(file_spec):
    try:
        resp = urllib.urlretrieve(file_spec["remote_fn"], file_spec["local_fn"])
        
        # archive.org returns HTML pages with status 200 for 404's. To tell if
        # we actually got a file, look for the ETag header, real files have them
        # and the "404" HTML page does not.
        if not "ETag" in resp[1]:
            os.unlink(file_spec["local_fn"])
            logging.error("Request returned 404 page for {0}".format(file_spec["remote_fn"]))
    except:
        logging.error("Unknown error downloading {0}".format(file_spec["remote_fn"]))


def get_all_urls(file_specs, workers):
    logging.info("Starting file download")
    pool = Pool(workers)
    pool.map(download_file, file_specs)


def read_barcodes(item_fn):
    '''
    Read items.txt file from the data dump as tsv and return a list of all the 
    barcodes from the barcode column.
    '''
    barcodes = []
    with open(item_fn) as item_file:
        item_tsv = unicodecsv.DictReader(item_file, dialect="excel-tab")   
        for item in item_tsv:
            barcodes.append(item["BarCode"])
            
    logging.info("Loaded {0} barcodes".format(len(barcodes)))
    return barcodes


def make_mirror_dir(mirror_dir):
    '''
    Create mirror directory if it does not exist
    '''
    if not os.path.isdir(mirror_dir):
        os.mkdir(mirror_dir)
    return True


def make_urls_to_get(barcodes, file_suffixes, source_url, mirror_dir):
    # build list of things that need getting
    
    file_specs = []
    line = 1 # header row counts for one, this counter reflects the line in item.txt
    for barcode in barcodes:
        line += 1
        for suffix in file_suffixes:
            try:
                local_fn = os.path.join(mirror_dir, barcode) + suffix
                if not os.path.exists(local_fn):
                    remote_fn = "/".join([source_url, barcode, barcode]) + suffix
                    file_specs.append({"local_fn": local_fn,
                                    "remote_fn": remote_fn})
            except:
                logging.warning("Issue with barcode {0} on line {1}".format(barcode, line))

    logging.info("Need to fetch {0} files".format(len(file_specs)))
    return file_specs


def main():
    item_fn = "data/data-20160516/item.txt"
    mirror_dir = "data/mirror"
    source_url = "http://archive.org/download"

    # This is the suffix of the file that contains a list of availible files for
    # a barcode and their MD5 sums.
    manifest_suffix = "_files.xml"
    
    # All types of files that need to be downloaded. See 
    # https://biodivlib.wikispaces.com/Data+Exports for a list of the different
    # files availible for each barcode.
    file_suffixes = ["_djvu.txt", manifest_suffix]
    
    workers = 4
    
    # logging is to a file depending on time into the mirror directory
    make_mirror_dir(mirror_dir)
    iso_date_fmt = "%Y-%m-%dT%H-%M-%S"
    now_for_files = time.strftime(iso_date_fmt)
    log_file = "{0}/fetch_files_{1}.log".format(mirror_dir, now_for_files)
    logging.basicConfig(filename=log_file, level=logging.INFO)
    logging.info("Starting at {0}".format(now_for_files))

    barcodes = read_barcodes(item_fn)
    file_specs = make_urls_to_get(barcodes, file_suffixes, source_url, mirror_dir)
    get_all_urls(file_specs[0:10], workers)

    now_for_files = time.strftime(iso_date_fmt)
    logging.info("Finished at {0}".format(now_for_files))

if __name__ == "__main__":
    main()