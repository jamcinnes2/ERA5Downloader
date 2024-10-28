# ERA5 data downloading tool
#
# 2024 by John McInnes for Ground Truth Trekking
#
# todo: use os.path.join

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


# map ERA5 single levels long variable names to short variable names
# era5_varnmap = {
#     '2m_temperature':           't2m'
# }


def e5_var_filename( e5_var:str, year:int ) -> str:
    efname = f'{e5_var}_{year}.nc'
    return efname


def form_cds_request( lat_n:float, long_e:float, e5_var:str, year:int ) -> dict:
    # ERA5 data is in a 0.25 degree grid.
    # DEBUG: for now download 2x2 area so Panoply can open the .nc file.
    # ...Panoply is nice for debugging. It cant open a 1x1 .nc file.
    area_coord = [lat_n, long_e-0.25, lat_n-0.25, long_e]
    print( 'area_coord NWSE: ' + repr(area_coord))
    cds_req = {
        "product_type": ["reanalysis"],
        "variable": [e5_var],
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
    pass


def hours_in_year(year):
    is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    return 24 * 366 if is_leap else 24 * 365


def download_is_complete( nc_filename:str, year:int, dt_end:datetime.datetime ) -> bool:
    try:
        ads = netCDF4.Dataset( nc_filename, mode="r", clobber=False )
    except OSError:
        print( f'Existing output {nc_filename} could not be opened!' )
        exit(-1)

    # see if the dataset has a full years worth of hours.
    num_hours = len(ads['valid_time'])

    # if the year is THIS year, the # of hours available is the CDS ERA5 embargo period
    if year == dt_end.year:
        dt_jan1 = datetime.datetime(dt_end.year, 1, 1, tzinfo=datetime.timezone.utc)
        td1 = dt_end - dt_jan1
        max_hours = td1.total_seconds() // 3600
        print( f'debug max_hours {max_hours} ({td1.total_seconds()/3600}) for {year}')
    else:
        max_hours = hours_in_year(year)
    return num_hours >= max_hours


def download_era5( grid_lat_n:float, grid_long_e:float,
                   loc_path:str,
                   e5_vars:list,
                   dt_end:datetime.datetime ):
    cds_dsname = 'reanalysis-era5-single-levels'
    #cds = cdsapi.Client()
    cds = cdsapi.Client(
        url=os.environ.get("CDSAPI_URL"),
        key=os.environ.get("CDSAPI_KEY"),
        quiet=True,
        debug=False,
        verify=None,
        timeout=80,
        progress=True,
        delete=True,
        retry_max=500,
        sleep_max=60 )
        # wait_until_complete=True,
        # info_callback=None, warning_callback=None, error_callback=None,
        # debug_callback=None, metadata=None, forget=False,
        # session=requests.Session())


    # for each variable
    for e5_var in e5_vars:
        # for each year from 1940
        years = list( range( 1940, dt_end.year + 1 ) )
        for year in reversed(years):
            output_fname = e5_var_filename( e5_var, year )
            dest_filename = f'{loc_path}/{output_fname}'

            # see if file already completely downloaded.
            # So if it exists and is > then some nonsense amount, and is complete # of hours.
            already_exists = os.path.isfile(dest_filename) and os.path.getsize(dest_filename) > 500
            if already_exists:
                if download_is_complete(dest_filename,year,dt_end):
                    print(f'{dest_filename} is complete. skipping')
                    continue

            print( f'downloading {e5_var} {year}...' )
            cds_req = form_cds_request( grid_lat_n, grid_long_e, e5_var, year )
            # download to temporary filename
            temp_filename = dest_filename + '.tempdl'
            cds.retrieve(cds_dsname, cds_req, temp_filename)
            ## rename completed download, thus 'marking' it completed
            os.rename( temp_filename, dest_filename )
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
def create_csv( loc_path:str, csv_fname:str, e5_vars:list, start_year:int, end_year:int ):
    # truncate & recreate any existing CSV
    #csv_fname = './output.csv'
    csv_file = open( csv_fname, 'w')
    header_str = 'datetime'
    for e5v in e5_vars:
        header_str += ',' + e5v
    print(header_str, file=csv_file)
    #csv_file.write( header_str+'\n' );

    era5_varmap={}   # this dict will be our ERA5 long to short name map

    # we will process one year at a time
    years = list( range( start_year, end_year + 1 ) )
    for year in years:
        yidx = year - start_year
        num_hours = 0
        print( f'output year {year}' )

        # open all our raw dataset netcdf files for this year. one for each ERA5 var
        rds_list = []
        for e5v in e5_vars:
            rds, rfname = open_nc_ro( loc_path, e5v, year )
            rds_list.append( rds )
            #print( f'debug: opened {rfname}' )

            # get the number of hours. make sure each file has the same amount
            nh_in_file = rds.dimensions['valid_time'].size
            if num_hours == 0:  num_hours=nh_in_file
            elif nh_in_file != num_hours:
                raise RuntimeError(f'num_hours mismatch looking for {num_hours}, got {nh_in_file} {rfname}.')

            # CDS doesn't give us a way to programmatically map a variable's long name
            # ..to it's short name. The long name is used in the CDS request and the
            # ..short namne is used within the netcdf file. We will use the following
            # ..process to figure out the variable short name ourselves and store them
            # ..in a dict. The process is elimnate the known/expected variables and
            # ..whatever is left is the short name. So we are assuming 1 var per file.
            known_var_names = ['expver','latitude','longitude','number','valid_time']
            for short_vname in rds.variables:
                if short_vname not in known_var_names:
                    era5_varmap[e5v] = short_vname

        # write this years data to the CSV file
        #print(f'debug: num_hours {num_hours}')
        for hidx in range(num_hours):
            epoch_sec = rds_list[0]['valid_time'][hidx]
            the_dt = datetime.datetime.fromtimestamp(epoch_sec, datetime.timezone.utc)
            csvline_str = the_dt.isoformat()
            #print( f'debug: the_dt {csvline_str}')
            var_idx = 0
            for e5short in era5_varmap.values():
                #e5short = era5_varmap[e5v]
                # indexing is list,var,valid_time,lat,long
                val = rds_list[var_idx][e5short][hidx][0][0]
                csvline_str += f',{val}'
                var_idx += 1
            print(csvline_str, file=csv_file)

        # close netcdf datasets for year
        for ads in rds_list:
            ads.close()

    csv_file.close()
    pass


def main():
    app_version = "0.6.0"
    current_time = datetime.datetime.now()
    cdsdn_path = './cdsdownload'
    cds_dsname = 'reanalysis-era5-single-levels'
    csvout_path = './csvoutput'

    # hello
    print( f'CDS ERA5 {cds_dsname} download tool v{app_version} **\n')

    # Initialize cmd line argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument( '--list-variables', action='store_true', help='list available ERA5 variables' )
    parser.add_argument( '--no-download', action='store_true', help='dont download anything' )
    parser.add_argument('latn', nargs='?', default=59.4385, type=float, help='latitude in decimal degrees north')
    parser.add_argument('longe', nargs='?', default=-151.7150, type=float, help='longitude in decimal degrees east')
    parser.add_argument( 'varname', nargs='?', default='xInvalidnamex', help='ERA5 variable name' )
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
        # load era5 variable names
        e5_grps_fn = 'era5_reanalysis_meta.json'
        #e5_grp_fn = 'test.json'
        with open( e5_grps_fn, 'r') as file1:
            e5_grps = json.load( file1 )

        for e5g in e5_grps:
            print( f'{e5g["label"]}' )
            for e5vname in e5g["values"]:
                print( f'\t{e5vname}' )
            print( '' )
        print('done.')
        exit()

    e5_varlist = [args.varname]

    # calcute datetime of newest data we can request from CDS (5 day embargo)
    dt_today = datetime.datetime.now(tz=datetime.timezone.utc)
    dt_end = dt_today - datetime.timedelta( days=5 )
    #dt_end_str = f'{dt_end.year}-{dt_end.month}-{dt_end.day}'
    print( 'ERA5 latest data available should be: ' + dt_end.isoformat() );

    # create the download dirs & output dirs if necessary
    loc_path = os.path.join(cdsdn_path,location_name)
    os.makedirs(loc_path, exist_ok=True)
    os.makedirs(csvout_path, exist_ok=True)

    # download CDS data for location
    if args.no_download == False:
        download_era5(grid_lat_n, grid_long_e, loc_path, e5_varlist, dt_end)
        print('download done.')

    # transform raw downloads into CSV
    #csvout_name = 'output.csv'
    csvout_name = location_name + '.csv'
    csvout_fullpath = os.path.join(csvout_path, csvout_name)
    create_csv(loc_path, csvout_fullpath, e5_varlist, 1970, dt_end.year)
    print('csv done.')


if __name__ == '__main__':
    main()
