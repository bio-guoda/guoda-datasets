from __future__ import print_function
import os
from pyspark import SQLContext
import pyspark.sql.types as types
import pyspark.ml.feature as feature

import unicodecsv
from dateutil.parser import parse

sc = SparkContext(appName="BHLParquet")
sqlContext = SQLContext(sc)

def as_int(s):
    return None if (s is None) or (len(s.strip()) is 0) else int(s)

def as_date(s):
    return None if (s is None) or (len(s.strip()) is 0) else parse(s)

def type_data_item(l):
    try:
        return (
            as_int(l["ItemID"]),
            as_int(l["TitleID"]),
            as_int(l["ThumbnailPageID"]),
            l["BarCode"],
            l["MARCItemID"],
            l["CallNumber"],
            l["VolumeInfo"],
            l["ItemURL"],
            l["LocalID"],
            l["Year"],
            l["InstitutionName"],
            l["ZQuery"],
            as_date(l["CreationDate"])
        )
    except Exception as e:
        print(e)
        #raise
        return False

def schema_item():
    return types.StructType([
        types.StructField("itemid", types.IntegerType(), True),
        types.StructField("titleid", types.IntegerType(), True),
        types.StructField("thumbnailpageid", types.IntegerType(), True),
        types.StructField("barcode", types.StringType(), True),
        types.StructField("marcitemid", types.StringType(), True),
        types.StructField("callnumber", types.StringType(), True),
        types.StructField("volumeinfo", types.StringType(), True),
        types.StructField("itemurl", types.StringType(), True),
        types.StructField("localid", types.StringType(), True),
        types.StructField("year", types.StringType(), True),
        types.StructField("institutionname", types.StringType(), True),
        types.StructField("zquery", types.StringType(), True),
        types.StructField("creationdate", types.DateType(), True)
        ])

def type_data_subject(l):
    try:
        return (
            int(l["TitleID"]),
            l["Subject"],
            parse(l["CreationDate"])
        )
    except:
        return False

def schema_subject():
    return types.StructType([
        types.StructField("titleid", types.IntegerType(), True),
        types.StructField("subject", types.StringType(), True),
        types.StructField("creationdate", types.DateType(), True)
        ])



# Read a file with python's csv reader into a df - single threaded and
# inefficient but csv reading is not garanteed to be line-paralizable
# and Python's parsing code is more known/hackable than Spark's
def t_gen(fn, parse_method):
    i = 1 # start row number at 1 due to header
    errors = 0
    with open(fn) as f:
        # encoding specified as 'utf-8-sig' since dumps have byte order mark
        f_tsv = unicodecsv.DictReader(f, encoding='utf-8-sig', dialect="excel-tab")
        for l in f_tsv:
            i += 1
            row = parse_method(l)
            if row is not False:
                yield row
            else:
                errors += 1
                print("Error with {0} on line {1}".format(l, i))
                if errors > 50:
                    print("Too many errors, stopping.")
                    break


def mk_ocr_fn(dir_name, barcode):
    return os.path.join(mirror_dir, barcode) + "_djvu.txt"

def get_ocr(barcode):
    try:
        with open(mk_ocr_fn(mirror_dir, barcode), 'r') as f:
            ocr_text = f.read()
    except Exception as e:
        #print(e)
        ocr_text = None
        
    return ocr_text  


mirror_dir = "data/mirror"
data_dir = "data/data-20160516"

fn = os.path.join(data_dir, "item.txt")
df = sqlContext.createDataFrame(t_gen(fn, type_data_item), schema_item())


# Optional limit for testing
df = df.sample(withReplacement=False, fraction=0.001)


get_ocr_udf = sql.udf(get_ocr, types.StringType())
df_ocr = df.withColumn("ocrtext", get_ocr_udf(df["barcode"]))

df_ocr.write.parquet("data/first_draft5.parquet")
