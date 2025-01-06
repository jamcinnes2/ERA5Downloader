### ERA5 hourly levels Point Sampler - A tool to download ERA5 climate variables for a given location on the globe.
e5tool.py is a Python 3 command line program that uses the cdsapi to download ERA5 data and output it as a CSV file. You can request any valid ERA5 variables. And you can request several variables at once. Data is requested 1 year at a time but parallel requests are made to speed up the download process. Downloaded data files are cached locally to speed up new requests. 

The program requests the latest data possible every time you run it (with respect to the 5 day ERA5 embargo period). But only once a day. For example say you run the program to get 2m_temperature. You will get the latest 2m_temperature data. Then an hour later you run it again to get 2m_temperature. The program will not try to download any new data yet as this would be redundant. It will use the cached download. Then a day later you run the program again. It will now download the newest 2m_temperature data.

#### ERA5 hourly request limits
The CDS server has a limit of 120000 'items' per request. Roughly speaking this means you might only be able to download a few variables at a time, depending on the variable.
https://confluence-test-dc.ecmwf.int/display/CUSF/CDSAPI+limitations+and+restrictions

The ERA5 Download form webpage can be used to explore request limits:
https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=download

### Installation
If desired, create and activate a python virtual environment to install the program in. This is optional.
The python library requirements are listed in requirements.txt. You can install these using pip or your OS package manager.

`pip install -r requirements.txt`

### CDS API key
You will need to signup with the Copernicus data service to get an access token. And you will need to agree to the licenses. See full instructions here https://cds.climate.copernicus.eu/how-to-api

The license agreements can be accessed from the ERA5 Download form webpage:
https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=download

NOTE: The CDS service has been back logged with users and is very slow. You can check the status here and here:
https://cds.climate.copernicus.eu/live
https://status.ecmwf.int/

### Usage
On BSDish systems like Mac & Linux make the e5tool.py file executable and run it directly.

```
chmod +x e5tool.py
./e5tool.py
```

Or invoke it using the python command.

`python3 e5tool.py`

Run the program using the -h switch for usage help.

```
> python e5tool.py -h
CDS ERA5 reanalysis-era5-single-levels download tool v0.8.0 **

usage: e5tool.py [-h] [--list-variables] [--no-download] [--var VAR] [--location-name LOCATION_NAME] [latn] [longe]

positional arguments:
  latn                  latitude in decimal degrees north
  longe                 longitude in decimal degrees east

options:
  -h, --help            show this help message and exit
  --list-variables      list available ERA5 variables
  --no-download         dont download anything
  --var VAR             ERA5 variable name. can use multiple times
  --location-name LOCATION_NAME
                        Give the output file a friendly name. ex Tyonek

```

Downloads are cached in a folder named "cdsdownload".

CSV files are output in a folder named "csvoutput".

### Examples
Get temperature and dewpoint at 48.8575, 2.3514 and save it in a csv file named Paris.csv:

`./e5tool.py --var 2m_temperature --var 2m_dewpoint_temperature --location-name Paris 48.8575 2.3514`

### About ERA5 variable names
You can view the list of ERA5 variables by using the --list-vars command line option:

`python e5tool.py --list-vars`

There is a bash script included that you can use to update the list. You shouldn't need to do this very often, if ever. Run the script and it will scrape the ERA5 wiki page and store the long and short variable names. That wiki page has some known errors, you can correct these by hardcoding the correction in the script. You will see some corrections in there already:

`get_era5_names.sh`
 
### Credits
Developed for Ground Truth Alaska 2024
