# Biodiverity Heritage Library (BHL) Data

This is the code for constructing a parquet file from BHL's publiclly availible 
data. BHL has two main data sources: a database dump of their relational 
metadata and the image and OCR files that are the publications. The parquet
includes selected metadata and the OCR text from BHL's processing of the 
publication.

## Pre-requisites

On Ubuntu 16.04, the following packages need to be added for Python:

```bash
apt-get install python3-unicodecsv python3-dateutil python3-gevent
```

## Obtaining the metadata

BHL makes availible a full database dump that appears to be updated on the first
of the month. They use static URLs for these updated dumps though so care must
be taken to do versioning and unique identification on our side.

1. Download the `data.zip` dump file from http://www.biodiversitylibrary.org/data
1. Rename the file based on the data you can see in the file listing to 
   `data-YYYYMMDD-HHmm.zip`
1. Place the file in the `data/` sub directory of this repo
1. Extract all the contents of the file with 
   `unzip data-YYYYMMDD-HHMM.zip -d data-YYYYMMDD-HHmm`


## Obtaining the OCR text data

The `fetch_files.py` script will maintain a mirror of the OCR files from 
http://archive.org/download . The types of files copied are described here in
https://biodivlib.wikispaces.com/Data+Exports and we are only interested in the
OCR related text. These files are a bit over 100 GB in total.

This download is done by constructing a URL using the barcode field in the 
metadata and the base URL that the BHL site uses for serving the files at
archive.org. This is not done through the BHL api which results in much faster
operations and according to other who have used the API, a more complete set
of OCR data.

1. Run `python3 fetch_files.py YYYYMMDD-HHmm`
1. Make sure the `data/mirror` directory exists and is reasonable
1. Check the data/mirror/*.log file(s) for errors

Note that `fetch_files.py` is idempotent, it will skip files already present.


## Constructing the parquet

Once the data is stored in the `data/` directory, building the parquet is just
the spark job in `build_parquet.py`. This takes about 90 minutes on a dual
quad-core machine or about 45 minutes on okapi. There are certainly areas where
things are single threaded and the memory required is at least the size of the
resulting parquet (60 GB at this point) so there is room for optimization.

1. Make sure the output directory `data/bhl-YYYYMMDD-HHmm.parquet` does not
   exist.
1. Run `spark-submit --driver-memory 128G build_parquet.py YYYYMMDD-HHmm`


## Known data issues

While codeing, found some issues with data (exported data.zip on 2016-05-16):

1. item.txt line 15547 seems to have an unescaped new line in the "local=..." field which causes
line 15548 to not parse properly. Python cvs.DictReader() will still load the line but the resulting dict will have
None for the barcode key.



## Old initial work, not represented in this repo

An initial exploration into the feasability of obtaining data:

First:
```bash
wget http://www.biodiversitylibrary.org/data/data.zip
mv data.zip data-20160516.zip
unzip data-20160516.zip
tail -181461 item.txt | head -15000 > items.txt
```

In Postgres:
```sql
create table item ( ItemID varchar(250), TitleID varchar(250), ThumbnailPageID varchar(250), BarCode varchar(250), MARCItemID varchar(250), CallNumber varchar(250), VolumeInfo varchar(250), ItemURL varchar(250), LocalID varchar(250), Year varchar(250), InstititionName varchar(250), ZQuery varchar(250), CreationDate varchar(20) );
copy item from '/home/mcollins/bhl/data/data-20160516/items.txt' with (format text);
```

Then:
```bash
for bc in `cat barcodes2` ; do url="http://www.archive.org/download/${bc}/${bc}_djvu.txt"; echo $bc; wget -q $url; sleep 1 ;  done
```

12,290 files and 9.0 GB of text files with OCR in them are now in my directory so most downloaded fine.

Basically the only issue I found was that on line like 15212, Postgres says that there is too much data when running the copy command so the tab separated file isn't perfectly clean.




