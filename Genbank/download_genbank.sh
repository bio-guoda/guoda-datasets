#!/bin/bash

if [ "A$1" != "A" ] ; then
    version=$1
else
    echo "Usage: $0 <version>"
    exit 64
fi

d="data/${version}"
mkdir -p ${d}
wget --quiet -O ${d}/index.html ftp://ftp.ncbi.nlm.nih.gov/genbank/
grep "bytes)" ${d}/index.html | grep ".seq.gz" | awk -F '"' '{print $2}' > ${d}/urls.txt
parallel --no-notice --jobs 10 wget -N --quiet -P ${d} {} < ${d}/urls.txt
