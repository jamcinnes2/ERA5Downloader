#!/bin/bash

#Explanation
#
#    tr -d '\n' delete newlines
#    's/<tr[^>]*>/\n/g' convert tr tags into newlines to break data into table rows
#    's/<[^>]*t[dh]>/,/g' convert closing td/th tags into commas
#    's/<[^>]*>//g' delete all other html tags

# download ERA5 wiki page to get the tables of ERA5 variable names
curl "https://confluence.ecmwf.int/plugins/viewsource/viewpagesrc.action?pageId=76414402" > era5_names.html 

# parse out the long and short variable names to a csv file
< era5_names.html tr -d '\n' | grep -Po '<table.*?</table>' | grep -i 'name in CDS' | sed -e 's/,/\ /g' -e 's/<tr[^>]*>/\n/g' -e 's/<[^>]*t[dh]>/,/g' -e 's/<[^>]*>//g' | cut -d',' -f4,5 | grep -E --invert-match  -i 'variable name|^\s*$' > era5_names.csv

# fix the variables that we know are wrong on the wiki page
sed -i 's/2m_temperature,2t/2m_temperature,t2m/' era5_names.csv
sed -i 's/2m_dewpoint_temperature,2d/2m_dewpoint_temperature,d2m/' era5_names.csv
