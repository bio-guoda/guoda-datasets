# Biodiverity Heritage Library (BHL) Data


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
