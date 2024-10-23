# ERA5 data downloading tool
#
# 2024 by John McInnes for Ground Truth Trekking
#

import cdsapi
import json
import argparse
import datetime
import os


def form_cds_request( lat_n:float, long_e:float, e5_vars:list, year:int ) -> dict:
    # ERA5 data is in a 0.25 degree grid.
    # DEBUG: for now download 2x2 area so Panoply can open the .nc file.
    # ...Panoply is nice for debugging. It cant open a 1x1 .nc file.
    area_coord = [lat_n, long_e, lat_n-0.25, long_e-0.25]
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
    pass


def main():
    # if len( sys.argv ) != 1:
    #     print( 'Takes 2 filenames as arguments!' )
    #     exit( -1 )
    #
    # fn1 = sys.argv[1]
    # fn2 = sys.argv[2]

    app_version = "0.5.0"
    current_time = datetime.datetime.now()
    cdsdn_path = './cdsdownload'
    #output_fname = 'download.nc'

    # hello
    print( f'CDS ERA5 data download tool v{app_version} **\n')

    # Initialize cmd line argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument( '--list-variables', action='store_true', help='list ERA5 variables' )
    parser.add_argument('latn', nargs='?', default=59.50, help='latitude in decimal degrees north')
    parser.add_argument('longe', nargs='?', default=-151.75, help='longitude in decimal degrees east')
    parser.add_argument( 'varname', nargs='?', default='xInvalidnamex', help='ERA5 variable name' )
    args = parser.parse_args()

    # to nearest quarter degree for ERA5 dataset
    latitude_n = args.latn
    longitude_e = args.longe

    # simple hash of lat&long for download pathname
    loc_latn = int(round(latitude_n * 100))
    loc_lone = int(round(longitude_e * 100))
    location_name = f'{loc_latn}N_{loc_lone}E'
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

    # create the download dirs if necessary
    loc_path = f'{cdsdn_path}/{location_name}'
    os.makedirs(loc_path, exist_ok=True)

    # what dates do we want
    dt_today = datetime.datetime.now()
    dt_end = dt_today - datetime.timedelta( days=5 )
    dt_end_str = f'{dt_end.year}-{dt_end.month}-{dt_end.day}'
    print( 'end date: ' + dt_end_str );

    # download something
    e5_var = args.varname
    cds = cdsapi.Client()
    cds_dsname = 'reanalysis-era5-single-levels'

    # for each year from 1940
    years = list( range( 1940, dt_end.year + 1 ) )
    for year in reversed(years):
        print( f'downloading {e5_var} {year}...' )
        output_fname = f'{e5_var}_{year}.nc'
        dest_filename = f'{loc_path}/{output_fname}'
        # see if file already downloaded. Exists and is > then some nonsense amount
        already_exists = os.path.isfile(dest_filename) and os.path.getsize(dest_filename) > 500
        if already_exists:
            print(f'{dest_filename} exists already. skipping')
            break

        cds_req = form_cds_request( lat_n, long_e, [e5_var], year );
        cds.retrieve(cds_dsname, cds_req, dest_filename)

    print('done.')

if __name__ == '__main__':
    main()
