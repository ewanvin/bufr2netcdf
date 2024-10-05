#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from unittest.mock import NonCallableMock
import pandas as pd
import xarray as xr
import yaml
import time
from itertools import dropwhile
import sys 
import os
import subprocess
from eccodes import *
import requests
import argparse
from io import StringIO
import logging
from logging.handlers import TimedRotatingFileHandler
import matplotlib.pyplot as plt
import numpy as np
import traceback
from SPARQLWrapper import SPARQLWrapper, JSON
import operator
import re
from dateutil.relativedelta import relativedelta
from funcs.get_keywords import *
from funcs.code_tables import *
from funcs.useful_functions import *
import netCDF4
import glob
from datetime import datetime, timedelta, date
from calendar import monthrange, month_name

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-c","--cfg",dest="cfgfile",
            help="Configuration file", required=True)
    parser.add_argument("-s","--startday",dest="startday",
            help="Start day in the form YYYY-MM-DD", required=False)
    parser.add_argument("-e","--endday",dest="endday",
            help="End day in the form YYYY-MM-DD", required=False)
    parser.add_argument("-t","--type",dest="stationtype",
            help="Choose between block, wigos or radio", required=True)
    parser.add_argument("-i", "--init", dest="initialize",
            help="Download all data", required=False, action="store_true")
    parser.add_argument("-u", "--update", dest="update",
            help="Adds the latest bufr-data to the netcdf-file", required=False, action="store_true")
    parser.add_argument("-a", "--all", dest="all_stations",
            help="To download/upload data from all stations", required=False, action="store_true")
    parser.add_argument("-st", "--station", nargs='*', default=[], dest="spec_station",
            help='To select specific stations. Must be in the from "st1 st2 .. stn". Must not be separated by comma', required=False)
    args = parser.parse_args()
    
    if args.startday is None:
        pass
    else:
        try:
            datetime.strptime(args.startday,'%Y-%m-%d')
        except ValueError:
            raise ValueError
        
    if args.endday is None:
            pass
    else:
        try:
            datetime.strptime(args.endday,'%Y-%m-%d')
        except ValueError:
            raise ValueError
        
    if args.cfgfile is None:
        parser.print_help()
        parser.exit()
        
    return args


def parse_cfg(cfgfile):

    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)
    return cfgstr

def get_files_specified_dates(desired_path):
    
    get_args = parse_arguments()
    startday = get_args.startday
    endday = get_args.endday
    path = parse_cfg(get_args.cfgfile)['station_info']['path']

    # open up path to files and sort the files such that the date is in chronological order
    files = []
    for file in os.listdir(desired_path):
        if file.endswith('.bufr'):
            files.append(datetime.strptime(file[5:-5], "%Y%m%d%H"))
    
    files = sorted(files)
    sorted_files = []
    for i in files:
        sorted_files.append(path + "/ocea_" + datetime.strftime(i, "%Y%m%d%H") + '.bufr')
    
    # get startday from parser
    start = parse_arguments().startday
    end = parse_arguments().endday
    
    start = start.replace('-','')
    end = end.replace('-','')
    
    startday = []       
    endday = [] 
    for i in sorted_files:
        if start in i:
           startday.append(i) 
        if end in i:
            endday.append(i)
            
    if len(startday) == 0:
        print('your startdate is out of range')
        sys.exit()
    if len(endday) == 0:
        print('your endday is out of range')
        sys.exit()
        
    startpoint_index = sorted_files.index(startday[0])
    endpoint_index = sorted_files.index(endday[-1]) + 1
    
    return sorted_files[startpoint_index:endpoint_index]


def get_files_initialize(desired_path):
    
    # gets a list of the files that we wish to look at

    files = []
    for file in os.listdir(desired_path):
        if file.endswith('.bufr'):
          # Create the filepath of particular file
            file_path = ('{}/{}'.format(desired_path,file))
            files.append(file_path)
    return files


def bufr_2_json(file):
    
    #open bufr file, convert to json (using "-j s") and load it with json.loads

    json_file = json.loads(subprocess.check_output(r"bufr_dump -j f {}".format(file), shell=True))
    
   
    # sorting so that each subset is a list of dictionary
    count = 0
    sorted_messages = []
    for message in json_file['messages']:
        if message['key'] == 'subsetNumber':
            count += 1
            sorted_messages.append([])
        else:
            try:
                sorted_messages[count-1].append({'key': message['key'], 'value': message['value'],
                                   'code': message['code'], 'units': message['units']})
            except:
                print('this did not work')
    
    # sorting so that height of measurement equipment is a variable attribute instead of variable
    final_message = []        
    for msg in sorted_messages:
        height_measurements = ['007006', '007030', '007031', '007032', '007033'] # might have to add more codes here, check: https://confluence.ecmwf.int/display/ECC/WMO%3D30+element+table
        count_height = 0
        storing_height = []
        variables_with_height = []
        for i in msg:
        
            # if variable is a height of sensor etc, check if it is none. If not none, then add it as an attribute
            # for the consecutive variables until next height of sensor.
            if i['code'] in height_measurements and i['value'] != None:
                count_height += 1
                storing_height.append(i)
            if i['code'] in height_measurements and i['value'] == None:
                count_height += 1
                storing_height.append(None)

            if count_height == 0:
                variables_with_height.append(i)
            if count_height != 0 and i['code'] not in height_measurements:
                if storing_height[count_height-1] != None and i['key'] != 'timePeriod':
                    i['height'] = storing_height[count_height-1]
                variables_with_height.append(i)
                
        final_message.append(variables_with_height)
    
        
    finale= []
    for msg in final_message:
        time_code = ['004024', '004025']
        count_time = 0
        variables_with_time = []
        storing_time = []
        for i in msg:
            if i['code'] in time_code and i['value'] != None:
                count_time += 1
                storing_time.append(i)
            if i['code'] in time_code and i['value'] == None:
                count_time += 1
                storing_time.append(None)

            if count_time == 0:
                variables_with_time.append(i)
            if count_time != 0 and i['code'] not in time_code:
                if storing_time[count_time-1] != None:
                    i['time'] = storing_time[count_time-1]
                variables_with_time.append(i)
        finale.append(variables_with_time) 
    return finale


def return_list_of_stations(get_files):
    cfg = parse_cfg(parse_arguments().cfgfile)
    stationtype = parse_arguments().stationtype
    stations = []
    
    if stationtype == 'ship':
        for one_file in get_files:
            simple_file = bufr_2_json(one_file)
            for station in simple_file:
                if station[0]['key'] == 'shipOrMobileLandStationIdentifier':
                    station_num = str(station[0]['value'])
                    if station_num not in stations:
                        stations.append(station_num)
    if stationtype == 'buoy':
        for one_file in get_files:
            simple_file = bufr_2_json(one_file)
            for station in simple_file:
                if station[0]['key'] == 'buoyOrPlatformIdentifier':
                    station_num = str(station[0]['value'])
                    if station_num not in stations:
                        stations.append(station_num)

    return stations

def sorting_hat(get_files, stations = 1):
    cfg = parse_cfg(parse_arguments().cfgfile)
    
    if not parse_arguments().spec_station and stations == 1:
        stations = return_list_of_stations(get_files)
    elif parse_arguments().spec_station and stations == 1:
        stations = parse_arguments().spec_station
        
    stations_dict = {i : [] for i in stations}
    
    for one_file in get_files:
        simple_file = bufr_2_json(one_file)
        for station in simple_file:
            if str(station[0]['value']) in stations:
                stations_dict['{}'.format(str(station[0]['value']))].append(station)
    
    print('bufr successfully converted to json')
    print('sorting hat completed')
    
    return stations_dict

def shipOrMobileLandStationIdentifier(msg):
    gathered_df = []
    gathered_df_units = []
    gathered_df_height = []
    gathered_df_time = []
    
    for i in msg:

        height_copy = copy_dict(i,'key','height')
        dict_copy = copy_dict(i, 'key','value', 'units', 'code')
        time_copy = copy_dict(i,'key','time')
        
        df = pd.DataFrame(dict_copy)

        #save a table with units for later use
        units_df = df
        units_df = units_df.drop(columns=['value'])
        cols = pd.io.parsers.base_parser.ParserBase({'names':units_df['key'], 'usecols':None})._maybe_dedup_names(units_df['key'])
        units_df['key'] = cols # will add a ".1",".2" etc for each double name

        #save a table with height of measurement for later use
        height_df = pd.DataFrame(height_copy)
        try:
            height_df['height_numb'] = [str(val.get('value')) + ' ' + str(val.get('units')) for val in height_df.height]
            height_df['height_type'] = [str(val.get('key')) for val in height_df.height]
            height_df = height_df.drop(columns=['height'])
            cols = pd.io.parsers.base_parser.ParserBase({'names':height_df['key'], 'usecols':None})._maybe_dedup_names(height_df['key'])
            height_df['key'] = cols # will add a ".1",".2" etc for each double name
            height_df = height_df.reset_index()
        except:
            height_df = height_df  

        #save a table with time of measurement for later use
        time_df = pd.DataFrame(time_copy)
        try:
            time_df['time_duration'] = [str(abs(val.get('value'))) + ' ' + str(val.get('units')) for val in time_df.time]
            time_df = time_df.drop(columns=['time'])
            cols = pd.io.parsers.base_parser.ParserBase({'names':time_df['key'], 'usecols':None})._maybe_dedup_names(time_df['key'])
            time_df['key'] = cols # will add a ".1",".2" etc for each double name
            time_df = time_df.reset_index()
        except:
            time_df = time_df
        
        df = df.transpose().reset_index()
        df.columns = df.iloc[0]
        df = df[1:-2]
        df['time'] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'minute']])#.dt.strftime('%Y-%m-%d %H:%M')
        df = df.set_index(['time'])

        #some columnnames repeat itself, creating problems when changing to xarray. Fix with:
        cols = pd.io.parsers.base_parser.ParserBase({'names':df.columns, 'usecols':None})._maybe_dedup_names(df.columns)
        df.columns = cols # will add a ".1",".2" etc for each double name
        df = df.drop(columns=['key', 'year', 'month', 'day', 'hour', 'minute'])
    
        gathered_df.append(df)
        gathered_df_units.append(units_df)
        gathered_df_height.append(height_df)
        gathered_df_time.append(time_df)
    #print(gathered_df)

    if len(gathered_df) == 0:
        print('There is no data to collect')
        sys.exit()
        
    if len(gathered_df_units) > 1:
        units_df = units(gathered_df_units).reset_index()
    else:
        units_df = gathered_df_units[0]
    
    if len(gathered_df_height) > 1:
        height_df = height(gathered_df_height).reset_index()
    else:
        height_df = gathered_df_height[0]
        
    if len(gathered_df_time) > 1:
        time_df = times(gathered_df_time).reset_index()
    else:
        time_df = gathered_df_time[0]
    
    if len(gathered_df) > 1:
        main_df = pd.concat(gathered_df)
    else:
        main_df = gathered_df[0]
        
    # need to check if the dfs are empty
    if height_df.empty:
        height_df = pd.DataFrame({'key': ['something','to','make', 'it' ,'pass']})
    if time_df.empty:
        time_df= pd.DataFrame({'key': ['something','to','make', 'it' ,'pass']})
    if units_df.empty:
        units_df= pd.DataFrame({'key': ['something','to','make', 'it' ,'pass']})

            
    # converting to numeric
    for column in main_df.columns:
        try:
            main_df[column] = pd.to_numeric(main_df[column])
        except:
            main_df[column] = main_df[column]
    
    #over to dataset
    main_ds = main_df.to_xarray()
    main_ds = main_ds.fillna(-9999)
    
    
    # some variables only need the first value as they are not dependent on time
    vars_one_val = ['shipOrMobileLandStationIdentifier', 'stationOrSiteName', 'stationType']
    for i in main_ds.keys():
        if i in vars_one_val:
            main_ds[i] = main_ds[i].isel(time=0)
    
    to_get_keywords = []
    
    # VARIABLE ATTRIBUTES
    for variable in main_ds.keys():
        
        
        var = variable
        if variable[-2] == '.':
            var = variable[:-2]
        change_upper_case = re.sub('(?<!^)(?=\d{2})', '_',re.sub('(?<!^)(?=[A-Z])', '_', var)).lower()
        
        
        
        # checking for standardname
        standardname_check = cf_match(change_upper_case)
        if standardname_check != 'fail':
            main_ds[variable].attrs['standard_name'] = standardname_check
        manual_stdname = {'windDirection': 'wind_from_direction', 'dewpointTemperature': 'dew_point_temperature',
                          'heightOfBaseOfCloud':'cloud_base_altitude', 
                          'characteristicOfPressureTendency':'tendency_of_air_pressure',
                          'horizontalVisibility':'visibility_in_air', 
                          'totalPrecipitationOrTotalWaterEquivalent': 'precipitation_amount',
                          'totalSnowDepth':'lwe_thickness_of_snowfall_amount'}
        if var in manual_stdname.keys():
            main_ds[variable].attrs['standard_name'] = manual_stdname[var]
        
        
        
        # adding long name. if variable has a height attribute, add it in the long name. if variable has a time attribute, add it in the long name.
        long_name = re.sub('_',' ', change_upper_case)
   
        if variable in height_df.key.values.tolist() and has_numbers(long_name) == False:
            main_ds[variable].attrs['long_name'] = (long_name + ' measured at ' + 
                                                    height_df.loc[height_df['key'] == variable, 'height_numb'].iloc[0])
        elif variable in time_df.key.values.tolist() and has_numbers(long_name) == False:
            main_ds[variable].attrs['long_name'] = (long_name + ' with time duration of ' + 
                                                    time_df.loc[time_df['key'] == variable, 'time_duration'].iloc[0])
        elif variable in height_df.key.values.tolist() and variable in time_df.key.values.tolist() and has_numbers(long_name) == False:
                        main_ds[variable].attrs['long_name'] = (long_name + ' measured at ' +
                                                                height_df.loc[height_df['key'] == variable, 'height_numb'].iloc[0] + 
                                                                ' with time duration of ' + 
                                                                time_df.loc[time_df['key'] == variable, 'time_duration'].iloc[0])
        else:
            main_ds[variable].attrs['long_name'] = long_name

        
        
        # adding units, if units is a CODE TABLE or FLAG TABLE, it will overrule the previous set attributes to set new ones
        if var in list(units_df['key']):
            main_ds[variable].attrs['units'] = units_df.loc[units_df['key'] == var, 'units'].iloc[0]
        else:
            continue    
            
        # adding units, if units is a CODE TABLE or FLAG TABLE, it will overrule the previous set attributes to set new ones
        if main_ds[variable].attrs['units'] == 'deg' and variable not in ['longitude', 'latitude']:
            main_ds[variable].attrs['units'] = 'degrees'
        elif variable == 'latitude' and main_ds[variable].attrs['units'] == 'deg':
            main_ds[variable].attrs['units'] = 'degrees_north'
        elif variable == 'longitude' and main_ds[variable].attrs['units'] == 'deg':
            main_ds[variable].attrs['units'] = 'degrees_east'
        elif main_ds[variable].attrs['units'] == 'Numeric':
            main_ds[variable].attrs['units'] = '1'
        if main_ds[variable].attrs['units'] == 'CODE TABLE':
            del main_ds[variable].attrs['units']
            main_ds[variable].attrs['long_name'] = main_ds[variable].attrs['long_name'] + ' according to WMO code table ' + units_df.loc[units_df['key'] == variable, 'code'].iloc[0]  
            main_ds[variable].attrs['units'] = '1'
        if main_ds[variable].attrs['units'] == 'FLAG TABLE':
            main_ds[variable].attrs['long_name'] = main_ds[variable].attrs['long_name'] + ' according to WMO flag table ' + units_df.loc[units_df['key'] == variable, 'code'].iloc[0]
            main_ds[variable].attrs['units'] = '1'

        # adding coverage_content_type
        thematicClassification = ['shipOrMobileLandStationIdentifier', 'stationOrSiteName', 'stationType']
        if variable in thematicClassification:
            main_ds[variable].attrs['coverage_content_type'] = 'thematicClassification'
        else:
            main_ds[variable].attrs['coverage_content_type'] = 'physicalMeasurement'
        to_get_keywords.append(change_upper_case)
        
        

        if variable[-2] == '.':
            new_name = variable.replace('.', '')
            main_ds[new_name] = main_ds[variable]
            main_ds = main_ds.drop([variable])
            
        to_get_keywords.append(change_upper_case)
        
        # must rename if variable name starts with a digit
        timeunits = ['hour', 'second', 'minute', 'year', 'month', 'day']
        if change_upper_case[0].isdigit() == True and change_upper_case.split('_')[1].lower() in timeunits:
            fixing_varname = re.sub( r"([A-Z])", r" \1", variable).split()
            fixing_varname = fixing_varname[2:] + fixing_varname[:2] + ['s']
            fixing_varname = ''.join(fixing_varname)
            fixing_varname = fixing_varname[0].lower() + fixing_varname[1:]
            main_ds[fixing_varname] = main_ds[variable]
            main_ds = main_ds.drop([variable])
        

    main_ds = main_ds.assign_coords({'latitude': main_ds['latitude'].values, 'longitude': main_ds['longitude'].values})

    main_ds['longitude'].attrs['long_name'] = 'longitude'
    main_ds['longitude'].attrs['standard_name'] = 'longitude'
    main_ds['longitude'].attrs['units'] = 'degrees_east'
    main_ds['longitude'].attrs['coverage_content_type'] = 'coordinate'

    main_ds['latitude'].attrs['long_name'] = 'latitude'
    main_ds['latitude'].attrs['standard_name'] = 'latitude'
    main_ds['latitude'].attrs['units'] = 'degrees_north'
    main_ds['latitude'].attrs['coverage_content_type'] = 'coordinate'

    
    ################# REQUIRED GLOBAL ATTRIBUTES ###################
    # this probably might have to be modified
    keywords, keywords_voc = get_keywords(to_get_keywords)
    cfg = parse_cfg(parse_arguments().cfgfile)
    
    main_ds.attrs['featureType'] = 'trajectory'
    if stationOrSiteName != None:
        main_ds.attrs['title'] = 'Measurements from {} with ship identifier number {}'.format(stationOrSiteName, shipOrMobileLandStationIdentifier) #
        main_ds.attrs['id'] = str(stationOrSiteName) + ', ' + str(shipOrMobileLandStationIdentifier)
    else:
        main_ds.attrs['title'] = 'Measurement from moving station with identifier number {}'.format(shipOrMobileLandStationIdentifier)
        main_ds.attrs['id'] = str(shipOrMobileLandStationIdentifier)
    main_ds.attrs['naming_authority'] = 'World Meteorological Organization'
    main_ds.attrs['source'] = cfg['output']['source']
    main_ds.attrs['summary'] = cfg['output']['abstract']
    today = date.today()

    main_ds.attrs['date_created'] = today.strftime('%Y-%m-%d %H:%M:%S')
    main_ds.attrs['geospatial_lat_min'] = '{:.3f}'.format(main_ds['latitude'].values.min())
    main_ds.attrs['geospatial_lat_max'] = '{:.3f}'.format(main_ds['latitude'].values.max())
    main_ds.attrs['geospatial_lon_min'] = '{:.3f}'.format(main_ds['longitude'].values.min())
    main_ds.attrs['geospatial_lon_max'] = '{:.3f}'.format(main_ds['longitude'].values.max())
    main_ds.attrs['time_coverage_start'] = main_ds['time'].values[0].astype('datetime64[s]').astype(datetime).strftime('%Y-%m-%d %H:%M:%S') # note that the datetime is changed to microsecond precision from nanosecon precision
    main_ds.attrs['time_coverage_end'] = main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime).strftime('%Y-%m-%d %H:%M:%S')
    
    duration_years = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).years)
    duration_months = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).months)
    duration_days = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).days)
    duration_hours = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).hours)
    duration_minutes = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).minutes)
    duration_seconds = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).seconds)
    main_ds.attrs['time_coverage_duration'] = ('P' + duration_years + 'Y' + duration_months +
                                               'M' + duration_days + 'DT' + duration_hours + 
                                               'H' + duration_minutes + 'M' + duration_seconds + 'S')    
    
    main_ds.attrs['keywords'] = keywords
    main_ds.attrs['keywords_vocabulary'] = keywords_voc
    main_ds.attrs['standard_name_vocabulary'] = 'CF Standard Name V79'
    main_ds.attrs['Conventions'] = 'ACDD-1.3, CF-1.6'
    main_ds.attrs['creator_type'] = cfg['author']['creator_type']
    main_ds.attrs['institution'] = cfg['author']['PrincipalInvestigatorOrganisation']
    main_ds.attrs['creator_name'] = cfg['author']['PrincipalInvestigator']
    main_ds.attrs['creator_email'] = cfg['author']['PrincipalInvestigatorEmail']
    main_ds.attrs['creator_url'] = cfg['author']['PrincipalInvestigatorOrganisationURL']
    main_ds.attrs['publisher_name'] = cfg['author']['Publisher']
    main_ds.attrs['publisher_email'] = cfg['author']['PublisherEmail']
    main_ds.attrs['publisher_url'] = cfg['author']['PublisherURL']
    main_ds.attrs['project'] = cfg['author']['Project']
    main_ds.attrs['license'] = cfg['author']['License']

    # this didnt work when I had it further up in the code :-) why?????
    main_ds = main_ds.assign({'trajectory': (('name_strlen'),np.array([shipOrMobileLandStationIdentifier]))})

    return main_ds


def buoyOrPlatformIdentifier(msg):
    tacos = []
    
    
    xars = []
    depth_ds = []
    other_ds = []
    df_units = []
    #print(msg)
    
    for i in msg:
        #print(i)
        count1 = 0
        count2 = 0
        #acos = []
   
        depth = []
        other = []
        for_units = []
       
     
        for j in i:    
            for_units.append({j['key']: {'units':j['units'], 'code':j['code']}})
            if j['key'] == 'depthBelowWaterSurface':
                count1 += 1
                depth.append({'{}'.format(j['value']):[]})
            elif count1 != 0:
                depth[count1-1]['{}'.format(str(depth[count1-1].keys())[12:-3])].append(j)
            else:
                other.append(j)
        
        df = pd.DataFrame(filter_section(other))
        #print(df)
        # need to filter of some more 
        df = df[['key','value','units','code']].copy()


        df_vals = df[['key','value']].copy()

        df_vals = df_vals.transpose()
        df_vals = df_vals.reset_index()
        df_vals = df_vals.rename(columns=df_vals.iloc[0])
        df_vals = df_vals.drop(df.index[0])
        
        try:
            blob = blob = (str(int(df_vals['year'][1])).zfill(4) + '-' + str(int(df_vals['month'][1])).zfill(2) + '-' + str(int(df_vals['day'][1])).zfill(2) + ' ' + str(int(df_vals['hour'][1])).zfill(2) + ':' +  str(int(df_vals['minute'][1])).zfill(2) + ':' + str(int(df_vals['second'][1])).zfill(2))
            blob = datetime.strptime(blob, "%Y-%m-%d %H:%M:%S")
        except:
            blob = (str(int(df_vals['year'][1])).zfill(4) + '-' + str(int(df_vals['month'][1])).zfill(2) + '-' + str(int(df_vals['day'][1])).zfill(2) + ' ' + str(int(df_vals['hour'][1])).zfill(2) + ':' +  str(int(df_vals['minute'][1])).zfill(2))
            blob = datetime.strptime(blob, "%Y-%m-%d %H:%M")

        
        for k in depth:
            for l in k:
                taco = pd.DataFrame.from_dict(k[l])
                taco = taco[['key','value']].copy().transpose().reset_index().drop(columns=['index'])
                header = taco.iloc[0]
                taco = taco[1:]
                taco.columns = header
                taco['depthBelowWaterSurface'] = k
                taco['index'] = count2
                taco = taco.set_index('index')
                tacos.append(taco)
                count2 += 1

                
        if len(tacos) > 1:
            full_taco = pd.concat(tacos[:-1])
        elif len(tacos) == 1:
            full_taco = tacos[0]
        else:
            print('could not extract data')
            return
        
        for column in full_taco.columns:
            try:
                full_taco[column] = pd.to_numeric(full_taco[column])
            except:
                full_taco[column] = full_taco[column]
                
        full_taco = full_taco.reset_index()
        full_taco = full_taco[full_taco.depthBelowWaterSurface != 'None']
        full_taco['time'] = [blob for i in range(len(full_taco['{}'.format(full_taco.columns[0])]))]
        full_taco = full_taco.set_index(['time','depthBelowWaterSurface'])
        full_taco = full_taco.fillna(-9999)
        depth_ds.append(full_taco)

        
            
        if 'second' in df_vals.columns:
            df_vals['time'] = blob
            df_vals = df_vals.drop(columns = ['key','year', 'month', 'day','hour', 'minute', 'second'])
        else:
            df_vals['time'] = blob
            df_vals = df_vals.drop(columns = ['key','year', 'month', 'day','hour', 'minute'])
            
        df_vals = df_vals.set_index('time')
        cols = pd.io.parsers.base_parser.ParserBase({'names':df_vals.columns, 'usecols':None})._maybe_dedup_names(df_vals.columns)

        df_vals.columns = cols # will add a ".1",".2" etc for each double name
        for column in df_vals.columns:
            try:
                df_vals[column] = pd.to_numeric(df_vals[column])
            except:
                df_vals[column] = df_vals[column]
        df_vals = df_vals.fillna(-9999)
        other_ds.append(df_vals)

        units_df = pd.DataFrame(for_units)

        new_unit = pd.DataFrame()
        for i in units_df.columns:
            try:
                vals = units_df[i].loc[~units_df[i].isnull()].iloc[0]
                new_unit.at['units','{}'.format(i)] = vals['units']
                new_unit.at['code','{}'.format(i)] = vals['code']

            except:
                continue
        new_unit = new_unit.transpose()
        new_unit = new_unit.reset_index().rename(columns={'index':'key'})
        cols = pd.io.parsers.base_parser.ParserBase({'names':new_unit['key'], 'usecols':None})._maybe_dedup_names(new_unit['key'])
        new_unit['key'] = cols # will add a ".1",".2" etc for each double name
        df_units.append(new_unit)

        
    if len(depth_ds) > 1:
        tik = pd.concat(depth_ds)
    elif len(depth_ds) == 1:
        tik = depth_ds[0]
    else:
        message = ('no data')
        return message
    
    tik = tik[~tik.index.duplicated()]
    tik = tik.to_xarray()

    if len(other_ds) > 1:
        tak = pd.concat(other_ds)
    elif len(other_ds) == 1:
        tak = other_ds[0]
    else:
        message = ('no data')
        return message
        
    tak = tak[~tak.index.duplicated()]
    tak = tak.to_xarray()
        
    tik = tik.drop_duplicates(dim="time")
    tak = tak.drop_duplicates(dim="time")
    
    main_ds = xr.combine_by_coords([tik, tak])

    if units_df.empty:
        units_df= pd.DataFrame({'key': ['something','to','make', 'it' ,'pass']})
    else:
        units_df = units(df_units)
    
    #main_ds = main_ds.sortby('time')
    # some variables only need the first value as they are not dependent on time
    vars_one_val = ['blockNumber', 'stationNumber','latitude',
                    'longitude', 'heightOfStation', 'wigosIdentifierSeries', 'wigosIssuerOfIdentifier',
                    'wigosIssueNumber','wigosLocalIdentifierCharacter']
    for i in main_ds.keys():
        if i in vars_one_val:
            main_ds[i] = main_ds[i].isel(time=0)
    
    to_get_keywords = []
    for variable in main_ds.keys():
        var = variable
        if variable[-2] == '.':
            var = variable[:-2]
        change_upper_case = re.sub('(?<!^)(?=\d{2})', '_',re.sub('(?<!^)(?=[A-Z])', '_', var)).lower()
        
        # checking for standardname
        standardname_check = cf_match(change_upper_case)
        if standardname_check != 'fail':
            main_ds[variable].attrs['standard_name'] = standardname_check
        
        # adding long name. if variable has a height attribute, add it in the long name. if variable has a time attribute, add it in the long name.
        long_name = re.sub('_',' ', change_upper_case)
        main_ds[variable].attrs['long_name'] = long_name
        
        # adding units, if units is a CODE TABLE or FLAG TABLE, it will overrule the previous set attributes to set new ones
        if variable in list(units_df['key']):
            main_ds[variable].attrs['units'] = units_df.loc[units_df['key'] == variable, 'units'].iloc[0]
        else:
            continue
        
        if main_ds[variable].attrs['units'] == 'deg':
            main_ds[variable].attrs['units'] = 'degrees'
        if main_ds[variable].attrs['units'] == 'Numeric':
            main_ds[variable].attrs['units'] = '1'
        if main_ds[variable].attrs['units'] == 'CODE TABLE':
            main_ds[variable].attrs['long_name'] = main_ds[variable].attrs['long_name'] + ' according to WMO code table ' + units_df.loc[units_df['key'] == variable, 'code'].iloc[0]  
            main_ds[variable].attrs['units'] = '1'
        elif main_ds[variable].attrs['units'] == 'FLAG TABLE':
            main_ds[variable].attrs['long_name'] = main_ds[variable].attrs['long_name'] + ' according to WMO flag table ' + units_df.loc[units_df['key'] == variable, 'code'].iloc[0]
            main_ds[variable].attrs['units'] = '1'

        # adding coverage_content_type
        thematicClassification = ['blockNumber', 'stationNumber', 'stationType']
        if variable in thematicClassification:
            main_ds[variable].attrs['coverage_content_type'] = 'thematicClassification'
        else:
            main_ds[variable].attrs['coverage_content_type'] = 'physicalMeasurement'
        #to_get_keywords.append(change_upper_case)

        # must rename if variable name starts with a digit
        timeunits = ['hour', 'second', 'minute', 'year', 'month', 'day']
        if change_upper_case[0].isdigit() == True and change_upper_case.split('_')[1].lower() in timeunits:
            fixing_varname = re.sub( r"([A-Z])", r" \1", variable).split()
            fixing_varname = fixing_varname[2:] + fixing_varname[:2] + ['s']
            fixing_varname = ''.join(fixing_varname)
            fixing_varname = fixing_varname[0].lower() + fixing_varname[1:]
            main_ds[fixing_varname] = main_ds[variable]
            main_ds = main_ds.drop([variable])
        
        # must also rename the ones that end with ".1"
        if variable[-2] == '.':
            new_name = variable.replace('.', '')
            main_ds[new_name] = main_ds[variable]
            main_ds = main_ds.drop([variable])
    
    #### GLOBAL ATTRIBUTES #####
    ################# REQUIRED GLOBAL ATTRIBUTES ###################
    # this probably might have to be modified
    cfg = parse_cfg(parse_arguments().cfgfile)
    main_ds.attrs['featureType'] = 'profile'
    
    
    main_ds.attrs['naming_authority'] = 'World Meteorological Organization (WMO)'
    main_ds.attrs['source'] = cfg['output']['source']
    main_ds.attrs['summary'] = cfg['output']['abstract']
    today = date.today()
    main_ds.attrs['history'] = today.strftime('%Y-%m-%d %H:%M:%S')+': Data converted from BUFR to NetCDF-CF'
    main_ds.attrs['date_created'] = today.strftime('%Y-%m-%d %H:%M:%S')
    main_ds.attrs['geospatial_lat_min'] = '{:.3f}'.format(main_ds['latitude'].values.min())
    main_ds.attrs['geospatial_lat_max'] = '{:.3f}'.format(main_ds['latitude'].values.max())
    main_ds.attrs['geospatial_lon_min'] = '{:.3f}'.format(main_ds['longitude'].values.min())
    main_ds.attrs['geospatial_lon_max'] = '{:.3f}'.format(main_ds['longitude'].values.max())
    main_ds.attrs['time_coverage_start'] = main_ds['time'].values[0].astype('datetime64[s]').astype(datetime).strftime('%Y-%m-%d %H:%M:%S') # note that the datetime is changed to microsecond precision from nanosecon precision
    main_ds.attrs['time_coverage_end'] = main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime).strftime('%Y-%m-%d %H:%M:%S')
    
    duration_years = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).years)
    duration_months = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).months)
    duration_days = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).days)
    duration_hours = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).hours)
    duration_minutes = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).minutes)
    duration_seconds = str(relativedelta(main_ds['time'].values[-1].astype('datetime64[s]').astype(datetime), main_ds['time'].values[0].astype('datetime64[s]').astype(datetime)).seconds)
    main_ds.attrs['time_coverage_duration'] = ('P' + duration_years + 'Y' + duration_months +
                                               'M' + duration_days + 'DT' + duration_hours + 
                                               'H' + duration_minutes + 'M' + duration_seconds + 'S')    
    
    #main_ds.attrs['keywords'] = keywords
    #main_ds.attrs['keywords_vocabulary'] = keywords_voc
    main_ds.attrs['standard_name_vocabulary'] = 'CF Standard Name V79'
    main_ds.attrs['Conventions'] = 'ACDD-1.3, CF-1.6'
    main_ds.attrs['creator_type'] = cfg['author']['creator_type']
    main_ds.attrs['institution'] = cfg['author']['PrincipalInvestigatorOrganisation']
    main_ds.attrs['creator_name'] = cfg['author']['PrincipalInvestigator']
    main_ds.attrs['creator_email'] = cfg['author']['PrincipalInvestigatorEmail']
    main_ds.attrs['creator_url'] = cfg['author']['PrincipalInvestigatorOrganisationURL']
    main_ds.attrs['publisher_name'] = cfg['author']['Publisher']
    main_ds.attrs['publisher_email'] = cfg['author']['PublisherEmail']
    main_ds.attrs['publisher_url'] = cfg['author']['PublisherURL']
    main_ds.attrs['project'] = cfg['author']['Project']
    main_ds.attrs['license'] = cfg['author']['License']
    
    print(main_ds)
    return main_ds   

def set_encoding(ds, fill=-9999, time_name = 'time', time_units='seconds since 1970-01-01 00:00:00'):
    
    all_encode = {}
        
    for v in list(ds.keys()):
        #if v == 'softwareVersionNumber':
         #   encode = {'zlib': True, 'complevel': 9, '_FillValue':fill}

        if 'float' in str(ds[v].dtype):
            dtip = 'f4'
            encode = {'zlib': True, 'complevel': 9, 'dtype': dtip, '_FillValue':fill}
        elif 'int' in str(ds[v].dtype):
            dtip = 'i4'
            encode = {'zlib': True, 'complevel': 9, 'dtype': dtip, '_FillValue':fill}
        else:
            encode = {'zlib': True, 'complevel': 9}
            
        all_encode[v] = encode
        
    coords = (list(ds.coords))
    for i in coords:
        all_encode[i] = {'zlib': True, 'complevel':9, 'dtype': 'f4', '_FillValue': fill}
    return all_encode

def saving_grace(file, key, destdir):
    file = file.sortby('time')
    gb = file.groupby('time.month')
    
    for group_name, group_da in gb:
        group_da = file
        t1 = group_da.time.isel(time=0).values.astype('datetime64[s]')
        t1 = t1.astype(datetime)

        t2 = group_da.time.isel(time=-1).values.astype('datetime64[s]')
        t2 = t2.astype(datetime)
        
        timestring1 = t1.strftime('%Y%m%dT%H%M%S')
        timestring2 = t2.strftime('%Y%m%dT%H%M%S')

        coords = (list(group_da.coords))#.remove('time')
        coords = [i for i in coords if i != 'time']
        #print(coords)
        #sys.exit()
        ds_dictio = group_da.to_dict()
        bad_time = ds_dictio['coords']['time']['data']
        ds_dictio['coords']['time']['data'] = np.array([((ti - datetime(1970,1,1)).total_seconds()) for ti in bad_time]).astype('i4')
        for i in coords:
            print(i)
            print(ds_dictio['coords'][i]['data'])
            ds_dictio['coords'][i]['data'] = np.float32(ds_dictio['coords'][i]['data'])
        all_ds_station_period = xr.Dataset.from_dict(ds_dictio)
        
        all_ds_station_period = all_ds_station_period.fillna(-9999)
        all_ds_station_period.to_netcdf('{}/ocea_{}_{}-{}.nc'.format(destdir, key, timestring1, timestring2),
                                            engine='netcdf4', encoding=set_encoding(all_ds_station_period))

if __name__ == "__main__":
    parse = parse_arguments()
    
    cfg = parse_cfg(parse.cfgfile)
    destdir = cfg['output']['destdir']
    frompath = cfg['station_info']['path']
    
    if parse.initialize:
        print('initialization has begun')
        sorted_files = sorting_hat(get_files_initialize(frompath))
        for key, val in sorted_files.items():
            if parse.stationtype == 'buoy':
                file = buoyOrPlatformIdentifier(sorted_files['{}'.format(key)])
            elif parse.stationtype == 'ship':
                file = shipOrMobileLandStationIdentifier(sorted_files['{}'.format(key)])
            sys.exit()
            saving = saving_grace(file, key, destdir)
        
               
    elif parse.update:
        print('updating...')
        
        # find the latest date that is stored in the folder
        
        stations = {}
        stations_names = []
        # get list of stationname in folder
        for file in os.listdir(destdir):
            if file.endswith('.nc'):
                file_split = file.split('_')
                if len(file_split[1])>5 and '-' not in file_split[1]:
                    if file_split[1] not in stations:
                        stations[file_split[1]] = [[file_split[2].split('-')[0], file_split[2].split('-')[1][:-3]]]
                        stations_names.append(file_split[1])
                    else:
                        stations[file_split[1]].append([file_split[2].split('-')[0], file_split[2].split('-')[1][:-3]])
                else:
                    continue
                    
        if parse.all_stations == True:            
            all_files = (return_list_of_stations(get_files_specified_dates(parse_cfg(parse.cfgfile)['station_info']['path'])))
        elif parse.spec_station:
            all_files = parse.spec_station
            

        for i in all_files:
            if i in stations_names:
                sorted_files = sorting_hat(get_files_specified_dates(parse_cfg(parse.cfgfile)['station_info']['path']), stations = [str(i)])
                if parse.stationtype == 'buoy':
                    file = buoyOrPlatformIdentifier(sorted_files['{}'.format(i)])
                elif parse.stationtype == 'ship':
                    file = shipOrMobileLandStationIdentifier(sorted_files['{}'.format(i)])
                # figuring out which is the last file for that station
                
                end_dates = []
                for k in stations[i]:
                    end_dates.append(datetime.strptime(k[1],'%Y%m%dT%H%M%S'))
                
                last_date = max(end_dates).strftime('%Y%m%dT%H%M%S')
                
                for k in stations[i]:
                    if last_date in k:
                        startday, endday = k
                
                #open latest dataset
                last_file = xr.open_dataset("{}/temp_{}_{}-{}.nc".format(destdir, i, startday, endday),decode_times=False)
                
                # must fix decoding thingy and name of dims in order to merge datasets together
                last_file_time = last_file.drop_dims('obs')
                last_file_pressure = last_file.drop_dims('profile')
                
                file_time = file.drop_dims('pressure')
                file_pressure = file.drop_dims('time')
                
                
                file_time = file_time.rename_dims({'time': 'profile'})
                file_pressure = file_pressure.rename_dims({'pressure': 'obs'})
                
                #rearrange dims for prof
                time = file_time.to_pandas()
                time['time'] = [to_datetime(ti) for ti in time['time']]
                
                last_file_time = last_file_time.to_pandas()
                last_file_time['time'] = [datetime.fromtimestamp(ti) for ti in last_file_time['time']]
                
                # making sure that the datasets have the same columns
                for o in list(time.columns):
                    if o not in list(last_file_time.columns):
                        time[o] = list(np.empty(len(time.index)))
                for o in list(last_file_time.columns):
                    if o not in list(time.columns):
                        last_file_time[o] = list(np.empty(len(last_file_time.index)))
                
                
                new_file_prof = pd.concat([time, last_file_time])
                new_file_prof = new_file_prof.sort_values(by=['time'])
                new_file_prof = new_file_prof.set_index('time')
                new_file_prof = xr.Dataset.from_dataframe(new_file_prof)
                
                #rearrange dims for obs
                pressure = file_pressure.to_pandas()
                last_file_pressure = last_file_pressure.to_pandas()
                
                for o in list(pressure.columns):
                    if o not in list(last_file_pressure.columns):
                        pressure[o] = list(np.empty(len(pressure.index)))
                for o in list(last_file_pressure.columns):
                    if o not in list(pressure.columns):
                        last_file_pressure[o] = list(np.empty(len(last_file_pressure.index)))
        
                new_file_obs = pd.concat([pressure, last_file_pressure])
                new_file_obs = new_file_obs.set_index('pressure')
                new_file_obs = xr.Dataset.from_dataframe(new_file_obs)
        
                # merging
                new_file = xr.merge([new_file_prof, new_file_obs],compat ='override')
        
                #finally remove the old file from destdir and save the new one
                last_file.close()
                os.remove("{}/ocea_{}_{}-{}.nc".format(destdir, i, startday, endday))
                
                #lastly save
                saving = saving_grace(new_file, i, destdir)
                
            else:
                continue

                
    elif parse.startday != None:
        print('creating files from {}'.format(parse.startday))
        
        sorted_files = sorting_hat(get_files_specified_dates(frompath))
        #print(sorted_files.keys)
        for key,val in sorted_files.items():
            if parse.stationtype == 'buoy':
                file = buoyOrPlatformIdentifier(sorted_files['{}'.format(key)])
            elif parse.stationtype == 'ship':
                file = shipOrMobileLandStationIdentifier(sorted_files['{}'.format(key)])
            #sys.exit()
            #saving = saving_grace(file, key, destdir)
            
    else:
        print('either type in -i, -u or enter starday/endday')
        sys.exit()
