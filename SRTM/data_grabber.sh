#!/bin/bash

wget -r http://viewfinderpanoramas.org/Coverage%20map%20viewfinderpanoramas_org3.htm

cd dem3
ls *.zip | xargs -L1 unzip

cd ../ANTDEM3
ls *.zip | xargs -L1 unzip

mv */*.hgt .