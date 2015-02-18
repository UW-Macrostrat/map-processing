rm -rf csvs

curl -LOk http://pubs.usgs.gov/of/2005/1323/data/ALcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/ARcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1305/data/AZcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1305/data/CAcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/COcsv.zip
curl -LOk http://pubs.usgs.gov/of/2006/1272/data/CTcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1325/data/DEcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1323/data/FLcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1323/data/GAcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/IAcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1305/data/IDcsv.zip
curl -LOk http://pubs.usgs.gov/of/2004/1355/data/ILcsv.zip
curl -LOk http://pubs.usgs.gov/of/2004/1355/data/INcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/KScsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1324/data/KYcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/LAcsv.zip
curl -LOk http://pubs.usgs.gov/of/2006/1272/data/MAcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1325/data/MDcsv.zip
curl -LOk http://pubs.usgs.gov/of/2006/1272/data/MEcsv.zip
curl -LOk http://pubs.usgs.gov/of/2004/1355/data/MIcsv.zip
curl -LOk http://pubs.usgs.gov/of/2004/1355/data/MNcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/MOcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1323/data/MScsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/MTcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1323/data/NCcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/NDcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/NEcsv.zip
curl -LOk http://pubs.usgs.gov/of/2006/1272/data/NHcsv.zip
curl -LOk http://pubs.usgs.gov/of/2006/1272/data/NJcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/NMcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1305/data/NVcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1325/data/NYcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1324/data/OHcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/OKcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1305/data/ORcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1325/data/PAcsv.zip
curl -LOk http://pubs.usgs.gov/of/2006/1272/data/RIcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1323/data/SCcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/SDcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1324/data/TNcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/TXcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1305/data/UTcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1325/data/VAcsv.zip
curl -LOk http://pubs.usgs.gov/of/2006/1272/data/VTcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1305/data/WAcsv.zip
curl -LOk http://pubs.usgs.gov/of/2004/1355/data/WIcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1324/data/WVcsv.zip
curl -LOk http://pubs.usgs.gov/of/2005/1351/data/WYcsv.zip

unzip \*.zip -d csvs
rm -f *.zip

psql geomacro < setup.sql

python import.py
python fix_text.py



