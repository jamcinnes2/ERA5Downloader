#!/usr/bin/env python3
# ERA5 data downloading tool
#
# 2024 by John McInnes for Ground Truth Alaska
#
# todo: use os.path.join, --no-download bug, useful errors on CDSAPI changes
# todo: use csv writer, be more efficient when downloading vars we already have

# some test variables.. the ones we used for hwitw
# test_variables = [
#     '10m_u_component_of_wind',
#     '10m_v_component_of_wind',
#     '2m_dewpoint_temperature',
#     '2m_temperature',
#     'cloud_base_height',
#     'precipitation_type',
#     'surface_pressure',
#     'total_cloud_cover',
#     'total_precipitation',
# ]

import cdsapi
import netCDF4
import json
import argparse
import datetime
import os
import math
import typing
import csv
import logging

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor


ERA5_START_YR: typing.Final = 1940
ERA5_NAMES_CSV: typing.Final = './era5_names.csv'
ERA5_KNOWN_METADATA: typing.Final = ['expver','latitude','longitude','number','valid_time']

# lookup the ERA5 single level variable's short name
# def e5_longtoshort( e5_var:str ) -> str:
#     with open(ERA5_NAMES_CSV, newline='') as csvfile:
#         reader = csv.reader(csvfile)
#         for row in reader:
#             if row[0] == e5_var:
#                 print( 'debug: long to short ' + e5_var + ' - ' + row[1])
#                 return row[1]
#     return None


def get_era5_names() -> dict:
    e5n = {}
    with open(ERA5_NAMES_CSV, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            e5n[row[0]] = row[1]
    return e5n


def e5_var_filename( e5_var:str, year:int ) -> str:
    efname = f'{e5_var}_{year}.nc'
    return efname


def form_cds_request( lat_n:float, long_e:float, e5_vars:list, year:int ) -> dict:
    # ERA5 data is in a 0.25 degree grid.
    # DEBUG: for now download 2x2 area so Panoply can open the .nc file.
    # ...Panoply is nice for debugging. It cant open a 1x1 .nc file.
    area_coord = [lat_n, long_e-0.25, lat_n-0.25, long_e]
    #print( 'debug: area_coord NWSE: ' + repr(area_coord))
    cds_req = {
        "product_type": ["reanalysis"],
        "variable": e5_vars,
        "year": [str(year)],
        "month": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ],
        "day": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12",
            "13", "14", "15",
            "16", "17", "18",
            "19", "20", "21",
            "22", "23", "24",
            "25", "26", "27",
            "28", "29", "30",
            "31"
        ],
        "time": [
            "00:00", "01:00", "02:00",
            "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00",
            "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00",
            "21:00", "22:00", "23:00"
        ],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": area_coord
    }
    return cds_req


def hours_in_year(year):
    is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    return 24 * 366 if is_leap else 24 * 365


def download_is_complete( nc_filename:str, year:int, dt_end:datetime.datetime ) -> bool:
    try:
        ads = netCDF4.Dataset( nc_filename, mode="r", clobber=False )
    except OSError as err:
        print( f'Existing output {nc_filename} could not be opened!\n{err}' )
        exit(-1)

    # see if the dataset has a full years worth of hours.
    num_hours = len(ads['valid_time'])
    ads.close()

    # if the year is THIS year, the # of hours available is the CDS ERA5 embargo period
    if year == dt_end.year:
        dt_jan1 = datetime.datetime(dt_end.year, 1, 1, tzinfo=datetime.timezone.utc)
        td1 = dt_end - dt_jan1
        max_hours = td1.total_seconds() // 3600
        logging.debug( f'{nc_filename} max_hours {max_hours} ({td1.total_seconds()/3600}) num_hours {num_hours} for {year}')
    else:
        max_hours = hours_in_year(year)

    return num_hours >= max_hours


# open an ERA5 nc file and delete the unwanted variables then save it
def strip_era5_vars( nc_filename:str, all_vars:list, keep_var:str ):
    import os
    import xarray
    # this makes the HDF5-DIAG messages go away
    xarray.set_options(file_cache_maxsize=400) # make it big enough for all files in mfdataset
    # from dask.distributed import Client
    # c = Client(n_workers=os.cpu_count()-2, threads_per_worker=1)

    # make list of short var names of variables we want to strip
    era5_names=get_era5_names()
    rem_e5_vars = []
    for e5v in all_vars:
        if e5v != keep_var:
            rem_e5_vars.append(era5_names[e5v])

    tempxds_filename = nc_filename + '.tempxds'

    xds = xarray.open_dataset(nc_filename)
    logging.debug( f'strip_era5_vars src {xds.variables} target {rem_e5_vars}' )
    xds_new = xds.drop_vars(rem_e5_vars)
    xds.close()
    xds_new.to_netcdf(tempxds_filename)
    xds_new.close()
    os.rename(tempxds_filename,nc_filename)


@python_app
def download_era5_year( grid_lat_n:float, grid_long_e:float,
                        loc_path:str,
                        e5_vars:list,
                        year:int,
                        quiet_flag:bool ):
    # internalize imports for parsl compat.
    import cdsapi
    #import netCDF4
    import logging
    import shutil
    import logging

    # eat the cds WARNING messages
    def warn_cback( astr:str ):
        pass

    # be really quiet if requested
    if quiet_flag:
        logging.disable(level=logging.CRITICAL+1)

    # download the data to a file named after var0
    output_fname = e5_var_filename( e5_vars[0], year )
    dest_filename = f'{loc_path}/{output_fname}'

    # setup CDS
    cds_dsname = 'reanalysis-era5-single-levels'
    cds = cdsapi.Client(
        url=os.environ.get("CDSAPI_URL"),
        key=os.environ.get("CDSAPI_KEY"),
        quiet=False,
        debug=False,
        verify=None,
        timeout=80,
        progress=True,
        delete=True,
        retry_max=5,
        sleep_max=60,
        warning_callback=warn_cback )
        # wait_until_complete=True,
        # info_callback=None, warning_callback=None, error_callback=None,
        # debug_callback=None, metadata=None, forget=False,
        # session=requests.Session())

    # download to temporary file
    print( f'requested {e5_vars} {year}...' )
    cds_req = form_cds_request( grid_lat_n, grid_long_e, e5_vars, year )
    temp_filename = dest_filename + '.tempdl'
    cds.retrieve(cds_dsname, cds_req, temp_filename)

    ## we downloaded all vars at once. make a copy of the downloaded file for each var,
    ## then strip the unwanted vars from it. So we aren't storing a bunch of redundant data.
    for e5v in e5_vars:
        d2_fname = e5_var_filename( e5v, year )
        d2_filename = f'{loc_path}/{d2_fname}'
        shutil.copy( temp_filename, d2_filename )
        logging.debug( 'debug: copied ' + temp_filename + " to " + d2_filename )
        # remove unused data from the new file
        strip_era5_vars( d2_filename, e5_vars, e5v )
    # done with this file
    os.remove(temp_filename)
    pass


def download_era5( grid_lat_n:float, grid_long_e:float,
                   loc_path:str,
                   e5_vars:list,
                   dt_end:datetime.datetime,
                   quiet_flag:bool):
    ## for each variable
    e5_vars_todownload = e5_vars.copy()
    for e5_var in e5_vars:
        lfut = []   # store the parsl futures
        # for each year from 1940
        years = list( range( ERA5_START_YR, dt_end.year + 1 ) )
        for year in reversed(years):
            # we are looking for this file
            the_fname = e5_var_filename( e5_var, year )
            the_destpathname = f'{loc_path}/{the_fname}'

            # see if file already completely downloaded.
            # So if it exists and is > then some nonsense amount, and is complete # of hours.
            already_exists = os.path.isfile(the_destpathname) and os.path.getsize(the_destpathname) > 500
            if already_exists:
                if download_is_complete(the_destpathname,year,dt_end):
                    logging.debug(f'{the_destpathname} is complete. skipping')
                    continue

            # use futures to download in parallel. request all vars at once
            lfut.append(
                download_era5_year( grid_lat_n, grid_long_e, loc_path, e5_vars_todownload, year, quiet_flag )
            )

        # Wait for the results from parsl-ing
        [i.result() for i in lfut]
        e5_vars_todownload.remove(e5_var)
        print(f'done downloading {e5_var}.')
    pass


# open raw netcdf readonly
def open_nc_ro( loc_path:str, e5_var:str, year:int ) -> (netCDF4.Dataset, str):
    # create & initialize the output dataset
    nc_filename = loc_path + '/' + e5_var_filename(e5_var,year)
    try:
        ads = netCDF4.Dataset( nc_filename, mode="r", clobber=False )
    except OSError:
        print( f'Existing output {nc_filename} could not be opened!' )
        exit(-1)

    return (ads, nc_filename)


# Create a CSV file from downloaded netcdf files for ONE location
def create_csv( loc_path:str, csv_fname:str, e5_vars:list, end_year:int ):
    # truncate & recreate any existing CSV
    csv_file = open( csv_fname, 'w' )

    # create the column header line (todo: we could include UNITS here)
    header_str = 'datetime'
    for e5v in e5_vars:
        header_str += ',' + e5v
    print(header_str, file=csv_file)

    # lookup the short var names, we will need them
    era5_names=get_era5_names()
    e5_short_vars = []
    for e5v in e5_vars:
        e5_short_vars.append(era5_names[e5v])

    # we will process one year at a time
    start_year = ERA5_START_YR
    years = list( range( start_year, end_year + 1 ) )
    for year in years:
        yidx = year - start_year
        print( f'\routputting year {year}     ', end='', flush=True )

        # open all our raw dataset netcdf files for this year. one for each ERA5 var
        rds_list = []
        rds_offset = []
        for e5v in e5_vars:
            rds, rfname = open_nc_ro( loc_path, e5v, year )
            rds_list.append( rds )

            first_epoch = rds['valid_time'][0]
            first_dt = datetime.datetime.fromtimestamp(first_epoch, datetime.timezone.utc)
            roffset_td = first_dt - datetime.datetime( year, 1, 1, tzinfo=datetime.timezone.utc )
            hour_offset = roffset_td / datetime.timedelta(hours=1)
            logging.debug( f'debug: {rfname} hour_offset {hour_offset}' )
            rds_offset.append( hour_offset )

            # get the number of hours in the file
            nh_in_file = rds.dimensions['valid_time'].size
            # look for strange things and warn
            if year == ERA5_START_YR:
                # with ERA5_START_YR there seems to be some variation. 8777 hours for some vars.
                if nh_in_file != 8760 and nh_in_file != 8784 and nh_in_file != 8777:
                    logging.debug(f'weird num_hours {nh_in_file} for {year} {rfname}.')

            elif year != end_year:
                if nh_in_file != 8760 and nh_in_file != 8784:
                    logging.debug(f'weird num_hours {nh_in_file} for {year} {rfname}.')

            # # CDS doesn't give us a way to programmatically map a variable's long name
            # # ..to it's short name. The long name is used in the CDS request and the
            # # ..short namne is used within the netcdf file. We will use the following
            # # ..process to figure out the variable short name ourselves and store them
            # # ..in a dict. The process is elimnate the known/expected variables and
            # # ..whatever is left is the short name. So we are assuming 1 var per file.
            # known_var_names = ERA_KNOWN_METADATA
            # for short_vname in rds.variables:
            #     if short_vname not in known_var_names:
            #         era5_varmap[e5v] = short_vname

        # write this years data to the CSV file
        #print(f'debug: num_hours {num_hours}')
        this_hour_dt = datetime.datetime( year, 1, 1, tzinfo=datetime.timezone.utc )
        end_hour_dt = datetime.datetime( year, 12, 31, hour=23, minute=0, tzinfo=datetime.timezone.utc )
        hour_idx = 0
        while this_hour_dt <= end_hour_dt:
            #epoch_sec = rds_list[0]['valid_time'][hidx] # these files use epoch time format
            #epoch_sec = this_hour_dt.timestamp()
            #the_dt = datetime.datetime.fromtimestamp(epoch_sec, datetime.timezone.utc)
            csvline_str = this_hour_dt.isoformat()
            #print( f'debug: the_dt {csvline_str}')
            var_idx = 0
            for e5short in e5_short_vars:    # have to use the short names here
                # indexing is list,var,valid_time,lat,long
                # select from e5_var where the time == epoch_sec
                rds_hour_idx = hour_idx - rds_offset[var_idx]
                if rds_hour_idx >= 0 and rds_hour_idx < rds_list[var_idx]['valid_time'].size:
                    val = rds_list[var_idx][e5short][rds_hour_idx][0][0]
                    val_str = str(val)
                else:
                    val_str = 'null'

                csvline_str += ',' + val_str
                var_idx += 1

            print(csvline_str, file=csv_file)
            this_hour_dt = this_hour_dt + datetime.timedelta( hours=1 )
            hour_idx += 1
            # if this_hour_dt.hour==0:
            #     print('.',end=None)

        # close netcdf datasets for year
        for ads in rds_list:
            ads.close()

    csv_file.close()
    pass


def main():
    app_version = "0.9.0"
    current_time = datetime.datetime.now()
    cdsdn_path = './cdsdownload'
    cds_dsname = 'reanalysis-era5-single-levels'
    csvout_path = './csvoutput'
    num_parallel_downloads = 5
    logging.basicConfig(level=logging.ERROR) #logging.DEBUG)
    # cdsapi has a bug that causes parsl to spit out alot of logging noise we dont want

    # Configure parsl to use a local thread pool.
    # Set the # of simultaneous downloads here! max_threads
    local_threads = Config(
        executors=[
            ThreadPoolExecutor( max_threads=num_parallel_downloads, label='local_threads')
        ],
        initialize_logging=False
    )
    parsl.clear()
    parsl.load(local_threads)
    logging.getLogger("parsl").setLevel(level=logging.CRITICAL+1) # disable parsl logging


    # hello
    print( f'CDS ERA5 {cds_dsname} download tool v{app_version} **\n')

    # Initialize cmd line argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument( '--list-variables', action='store_true', help='list available ERA5 variables' )
    parser.add_argument( '--no-download', action='store_true', help='dont download anything' )
    parser.add_argument( '--verbose', action='store_true', help='verbose CDSAPI messages' )
    parser.add_argument( 'latn', nargs='?', default=59.4385, type=float, help='latitude in decimal degrees north')
    parser.add_argument( 'longe', nargs='?', default=-151.7150, type=float, help='longitude in decimal degrees east')
    parser.add_argument( '--var', action='append', help='ERA5 variable name. can use multiple times' )
    parser.add_argument( '--location-name', help='Give the output file a friendly name. ex Tyonek' )
    args = parser.parse_args()

    # to nearest quarter degree for ERA5 dataset
    latitude_n = args.latn
    longitude_e = args.longe
    # simple hash of lat&long for download pathname
    grid_lat_n = math.ceil( latitude_n * 4 ) / 4        # round to nearest qtr deg
    grid_long_e = math.floor( longitude_e * 4) / 4
    assert grid_lat_n >= -90.0, grid_lat_n <= 90.0
    assert grid_long_e >= -180.0, grid_long_e <= 180.0
    loc_latn = int(round(grid_lat_n * 100))             # move decimal 2 right
    loc_lone = int(round(grid_long_e * 100))
    location_name = f'{loc_latn}N_{loc_lone}E'
    print(f'Using location: {grid_lat_n}N {grid_long_e}E')
    print(f'Using location name: {location_name}')

    # if requested - list the era5 variables we know and quit
    if args.list_variables:
        print('------------------------')
        e5n = get_era5_names()
        for e5v,e5vs in e5n.items():
            print (e5v + ", " + e5vs)
        exit()

    if args.var == None:
        print( 'Error: You must give at least one variable name.' )
        exit(-1)
    e5_varlist = args.var

    quiet_flag = not args.verbose # if not verbose then be quiet
    if quiet_flag == True:
        logging.disable(level=logging.CRITICAL+1)
    else:
        logging.getLogger().setLevel(level=logging.INFO)

    # calcute datetime of newest data we can request from CDS (5 day embargo)
    dt_today = datetime.datetime.now(tz=datetime.timezone.utc)
    dt_end = dt_today - datetime.timedelta( days=5 )
    # step one day at a time.. dont try to get every new hour that passes
    dt_end = datetime.datetime( dt_end.year, dt_end.month, dt_end.day, tzinfo=datetime.timezone.utc )
    #dt_end_str = f'{dt_end.year}-{dt_end.month}-{dt_end.day}'
    print( 'ERA5 latest data available should be: ' + dt_end.isoformat() );

    # create the download dirs & output dirs if necessary
    loc_path = os.path.join(cdsdn_path,location_name)
    os.makedirs(loc_path, exist_ok=True)
    os.makedirs(csvout_path, exist_ok=True)

    # download CDS data for the given location
    if args.no_download == False:
        download_era5(grid_lat_n, grid_long_e, loc_path, e5_varlist, dt_end, quiet_flag)
        print('download done.')

    # transform the raw downloads into CSV
    friendly_name = location_name
    if args.location_name:
        friendly_name = args.location_name
    csvout_name = friendly_name + '.csv'
    csvout_fullpath = os.path.join(csvout_path, csvout_name)
    create_csv(loc_path, csvout_fullpath, e5_varlist, dt_end.year)
    print('csv done.')


if __name__ == '__main__':
    main()
