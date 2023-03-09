import numpy
import pandas

# store some of the most important tables
# might be better to make this a class
def table_info(code):
    name_of_table = {'020003': {'long_name': 'PRESENT WEATHER',
                                '_FillValue': '511',
                                'valid_range': '0, 510'},
                     '010063': {'long_name': 'CHARACTERISTIC OF PRESSURE TENDENCY',
                                '_FillValue': '15',
                                'valid_range': '0, 8'},
                     '020004': {'long_name': 'PAST WEATHER(1)',
                                '_FillValue': '31',
                                'valid_range': '0, 19'},
                     '020005': {'long_name': 'PAST WEATHER(2)',
                                '_FillValue': '31',
                                'valid_range': '0, 19'},
                     '002001': {'long_name': 'STATION TYPE',
                                '_FillValue': '3',
                                'valid_range': '0, 2'}
                     }
    return name_of_table.get(code)

#function_name = "table_info"
#result = eval(function_name + "('020003')")
#print(result)


def table_020003(values):
    present_weather = {'0': 'CLOUD DEVELOPMENT NOT OBSERVED OR NOT OBSERVABLE',
                '1': 'CLOUDS GENERALLY DISSOLVING OR BECOMING LESS DEVELOPED',
                '2': 'STATE OF SKY ON THE WHOLE UNCHANGED',
                '3': 'CLOUDS GENERALLY FORMING OR DEVELOPING',
                '4': 'VISIBILITY REDUCED BY SMOKE, E.G. VELDT OR FOREST FIRES, INDUSTRIAL SMOKE OR VOLCANIC ASHES',
                '5': 'HAZE',
                '6': 'WIDESPREAD DUST IN SUSPENSION IN THE AIR, NOT RAISED BY WIND AT OR NEAR THE STATION AT THE TIME OF OBSERVATION',
                '7': 'DUST OR SAND RAISED BY WIND AT OR NEAR THE STATION AT THE TIME OF OBSERVATION, BUT NO WELL-DEVELOPED DUST WHIRL(S) OR SAND WHIRL(S), AND NO DUSTSTORM',
                '8': 'WELL-DEVELOPED DUST WHIRL(S) OR SAND WHIRL(S) SEEN AT OR NEAR THE STATION DURING THE PRECEDING HOUR OR AT THE SAME TIME OF OBSERVATION, BUT NO DUSTSTO',
                '9': 'DUSTSTORM OR SANDSTORM WITHIN SIGHT AT THE TIME OF OBSERVATION, OR AT THE STATION DURING THE PRECEDING HOUR',
                '10': 'MIST',
                '11': 'PATCHES',
                '12': 'MORE OR LESS CONTINUOUS',
                '13': 'LIGHTNING VISIBLE, NO THUNDER HEARD',
                '14': 'PRECIPITATION WITHIN SIGHT, NOT REACHING THE GROUND OR THE SURFACE OF THE SEA',
                '15': 'PRECIPITATION WITHIN SIGHT, REACHING THE GROUND OR THE SURFACE OF THE SEA, BUT DISTANT, I.E. ESTIMATED TO BE MORE THAN 5 KM FROM THE STATION',
                '16': 'PRECIPITATION WITHIN SIGHT, REACHING THE GROUND OR THE SURFACE OF THE SEA, NEAR TO, BUT NOT AT THE STATION',
                '17': 'THUNDERSTORM, BUT NO PRECIPITATION AT THE TIME OF OBSERVATION',
                '18': 'SQUALLS',
                '19': 'FUNNEL CLOUD(S)',
                '20': 'DRIZZLE (NOT FREEZING) OR SNOW GRAINS',
                '21': 'RAIN (NOT FREEZING)',
                '22': 'SNOW',
                '23': 'RAIN OR SNOW OR ICE PELLETS',
                '24': 'FREEZING DRIZZLE OR FREEZING RAIN',
                '25': 'SHOWER(S) OF RAIN',
                '26': 'SHOWER(S) OF SNOW, OR OF RAIN AND SNOW',
                '27': 'SHOWER(S) OF HAIL*, OR OF RAIN AND HAIL*',
                '28': 'FOG, OR ICE FOG',
                '29': 'THUNDERSTORM (WITH OR WITHOUT PRECIPITATION)',
                '30': 'SLIGHT OR MODERATE DUSTSTORM OR SANDSTORM',
                '31': 'SLIGHT OR MODERATE DUSTSTORM OR SANDSTORM',
                '32': 'SLIGHT OR MODERATE DUSTSTORM OR SANDSTORM',
                '33': 'SEVERE DUSTSTORM OR SANDSTORM',
                '34': 'SEVERE DUSTSTORM OR SANDSTORM',
                '35': 'SEVERE DUSTSTORM OR SANDSTORM',
                '36': 'SLIGHT OR MODERATE DRIFTING SNOW',
                '37': 'HEAVY DRIFTING SNOW',
                '38': 'SLIGHT OR MODERATE BLOWING SNOW',
                '39': 'HEAVY BLOWING SNOW',
                '40': 'FOG OR ICE FOG AT A DISTANCE AT THE TIME OF OBSERVATION, BUT NOT AT THE STATION DURING THE PRECEDING HOUR, THE FOG OR ICE FOG EXTENDING TO A LEVEL ABO',
                '41': 'FOG OR ICE FOG IN PATCHES',
                '42': 'FOG OR ICE FOG, SKY VISIBLE',
                '43': 'FOG OR ICE FOG, SKY INVISIBLE',
                '44': 'FOG OR ICE FOG, SKY VISIBLE',
                '45': 'FOG OR ICE FOG, SKY INVISIBLE',
                '46': 'FOG OR ICE FOG, SKY VISIBLE',
                '47': 'FOG OR ICE FOG, SKY INVISIBLE',
                '48': 'FOG, DEPOSITING RIME, SKY VISIBLE',
                '49': 'FOG, DEPOSITING RIME, SKY INVISIBLE',
                '50': 'DRIZZLE, NOT FREEZING, INTERMITTENT',
                '51': 'DRIZZLE, NOT FREEZING, CONTINUOUS',
                '52': 'DRIZZLE, NOT FREEZING, INTERMITTENT',
                '53': 'DRIZZLE, NOT FREEZING, CONTINUOUS',
                '54': 'DRIZZLE, NOT FREEZING, INTERMITTENT',
                '55': 'DRIZZLE, NOT FREEZING, CONTINUOUS',
                '56': 'DRIZZLE, FREEZING, SLIGHT',
                '57': 'DRIZZLE, FREEZING, MODERATE OR HEAVY (DENSE)',
                '58': 'DRIZZLE AND RAIN, SLIGHT',
                '59': 'DRIZZLE AND RAIN, MODERATE OR HEAVY',
                '60': 'RAIN, NOT FREEZING, INTERMITTENT',
                '61': 'RAIN, NOT FREEZING, CONTINUOUS',
                '62': 'RAIN, NOT FREEZING, INTERMITTENT',
                '63': 'RAIN, NOT FREEZING, CONTINUOUS',
                '64': 'RAIN, NOT FREEZING, INTERMITTENT',
                '65': 'RAIN, NOT FREEZING, CONTINUOUS',
                '66': 'RAIN, FREEZING, SLIGHT',
                '67': 'RAIN, NOT FREEZING, CONTINUOUS',
                '68': 'RAIN OR DRIZZLE AND SNOW, SLIGHT',
                '69': 'RAIN OR DRIZZLE AND SNOW, MODERATE OR HEAVY',
                '70': 'INTERMITTENT FALL OF SNOWFLAKES',
                '71': 'CONTINUOUS FALL OF SNOWFLAKES',
                '72': 'INTERMITTENT FALL OF SNOWFLAKES',
                '73': 'CONTINUOUS FALL OF SNOWFLAKES',
                '74': 'INTERMITTENT FALL OF SNOWFLAKES',
                '75': 'CONTINUOUS FALL OF SNOWFLAKES',
                '76': 'DIAMOND DUST (WITH OR WITHOUT FOG)',
                '77': 'SNOW GRAINS (WITH OR WITHOUT FOG)',
                '78': 'ISOLATED STAR-LIKE SNOW CRYSTALS (WITH OR WITHOUT FOG)',
                '79': 'ICE PELLETS',
                '80': 'RAIN SHOWER(S), SLIGHT',
                '81': 'RAIN SHOWER(S), MODERATE OR HEAVY',
                '82': 'RAIN SHOWER(S), VIOLENT',
                '83': 'SHOWER(S) OF RAIN AND SNOW MIXED, SLIGHT',
                '84': 'SHOWER(S) OF RAIN AND SNOW MIXED, MODERATE OR HEAVY',
                '85': 'SNOW SHOWER(S), SLIGHT',
                '86': 'SNOW SHOWER(S), MODERATE OR HEAVY',
                '87': 'SHOWER(S) OF SNOW PELLETS OR SMALL HAIL, WITH OR WITHOUT RAIN OR RAIN AND SNOW MIXED',
                '88': 'SHOWER(S) OF SNOW PELLETS OR SMALL HAIL, WITH OR WITHOUT RAIN OR RAIN AND SNOW MIXED',
                '89': 'SHOWER(S) OF HAIL, WITH OR WITHOUT RAIN OR RAIN AND SNOW MIXED, NOT ASSOCIATED WITH THUNDER',
                '90': 'SHOWER(S) OF HAIL, WITH OR WITHOUT RAIN OR RAIN AND SNOW MIXED, NOT ASSOCIATED WITH THUNDER',
                '91': 'SLIGHT RAIN AT TIME OF OBSERVATION',
                '92': 'MODERATE OR HEAVY RAIN AT TIME OF OBSERVATION',
                '93': 'SLIGHT SNOW, OR RAIN AND SNOW MIXED OR HAIL* AT TIME OF OBSERVATION',
                '94': 'MODERATE OR HEAVY SNOW, OR RAIN AND SNOW MIXED OR HAIL* AT TIME OF OBSERVATION',
                '95': 'THUNDERSTORM, SLIGHT OR MODERATE, WITHOUT HAIL*, BUT WITH RAIN AND/OR SNOW AT TIME OF OBSERVATION',
                '96': 'THUNDERSTORM, SLIGHT OR MODERATE, WITH HAIL* AT TIME OF OBSERVATION',
                '97': 'THUNDERSTORM, HEAVY, WITHOUT HAIL*, BUT WITH RAIN AND/OR SNOW AT TIME OF OBSERVATION',
                '98': 'THUNDERSTORM COMBINED WITH DUSTSTORM OR SANDSTORM AT TIME OF OBSERVATION',
                '99': 'THUNDERSTORM, HEAVY, WITH HAIL* AT TIME OF OBSERVATION',
                '100': 'NO SIGNIFICANT WEATHER OBSERVED',
                '101': 'CLOUDS GENERALLY DISSOLVING OR BECOMING LESS DEVELOPED DURING THE PAST HOUR',
                '102': 'STATE OF SKY ON THE WHOLE UNCHANGED DURING THE PAST HOUR',
                '103': 'CLOUDS GENERALLY FORMING OR DEVELOPING DURING THE PAST HOUR',
                '104': 'HAZE OR SMOKE, OR DUST IN SUSPENSION IN THE AIR, VISIBILITY EQUAL TO, OR GREATER THAN, 1 KM',
                '105': 'HAZE OR SMOKE, OR DUST IN SUSPENSION IN THE AIR, VISIBILITY LESS THAN 1 KM',
                '110': 'MIST',
                '111': 'DIAMOND DUST',
                '112': 'DISTANT LIGHTNING',
                '118': 'SQUALLS',
                '119': 'RESERVED',
                '120': 'FOG',
                '121': 'PRECIPITATION',
                '122': 'DRIZZLE (NOT FREEZING) OR SNOW GRAINS',
                '123': 'RAIN (NOT FREEZING)',
                '124': 'SNOW',
                '125': 'FREEZING DRIZZLE OR FREEZING RAIN',
                '126': 'THUNDERSTORM (WITH OR WITHOUT PRECIPITATION)',
                '127': 'BLOWING OR DRIFTING SNOW OR SAND',
                '128': 'BLOWING OR DRIFTING SNOW OR SAND, VISIBILITY EQUAL TO, OR GREATER THAN, 1 KM',
                '129': 'BLOWING OR DRIFTING SNOW OR SAND, VISIBILITY LESS THAN 1 KM',
                '130': 'FOG',
                '131': 'FOG OR ICE FOG IN PATCHES',
                '132': 'FOG OR ICE FOG, HAS BECOME THINNER DURING THE PAST HOUR',
                '133': 'FOG OR ICE FOG, NO APPRECIABLE CHANGE DURING THE PAST HOUR',
                '134': 'FOG OR ICE FOG, HAS BEGUN OR BECOME THICKER DURING THE PAST HOUR',
                '135': 'FOG, DEPOSITING RIME',
                '140': 'PRECIPITATION',
                '141': 'PRECIPITATION, SLIGHT OR MODERATE',
                '142': 'PRECIPITATION, HEAVY',
                '143': 'LIQUID PRECIPITATION, SLIGHT OR MODERATE',
                '144': 'LIQUID PRECIPITATION, HEAVY',
                '145': 'SOLID PRECIPITATION, SLIGHT OR MODERATE',
                '146': 'SOLID PRECIPITATION, HEAVY',
                '147': 'FREEZING PRECIPITATION, SLIGHT OR MODERATE',
                '148': 'FREEZING PRECIPITATION, HEAVY',
                '149': 'RESERVED',
                '150': 'DRIZZLE',
                '151': 'DRIZZLE, NOT FREEZING, SLIGHT',
                '152': 'DRIZZLE, NOT FREEZING, MODERATE',
                '153': 'DRIZZLE, NOT FREEZING, HEAVY',
                '154': 'DRIZZLE, FREEZING, SLIGHT',
                '155': 'DRIZZLE, FREEZING, MODERATE',
                '156': 'DRIZZLE, FREEZING, HEAVY',
                '157': 'DRIZZLE AND RAIN, SLIGHT',
                '158': 'DRIZZLE AND RAIN, MODERATE OR HEAVY',
                '159': 'RESERVED',
                '160': 'RAIN',
                '161': 'RAIN, NOT FREEZING, SLIGHT',
                '162': 'RAIN, NOT FREEZING, MODERATE',
                '163': 'RAIN, NOT FREEZING, HEAVY',
                '164': 'RAIN, FREEZING, SLIGHT',
                '165': 'RAIN, FREEZING, MODERATE',
                '166': 'RAIN, FREEZING, HEAVY',
                '167': 'RAIN (OR DRIZZLE) AND SNOW, SLIGHT',
                '168': 'RAIN (OR DRIZZLE) AND SNOW, MODERATE OR HEAVY',
                '169': 'RESERVED',
                '170': 'SNOW',
                '171': 'SNOW, SLIGHT',
                '172': 'SNOW, MODERATE',
                '173': 'SNOW, HEAVY',
                '174': 'ICE PELLETS, SLIGHT',
                '175': 'ICE PELLETS, MODERATE',
                '176': 'ICE PELLETS, HEAVY',
                '177': 'SNOW GRAINS',
                '178': 'ICE CRYSTALS',
                '179': 'RESERVED',
                '180': 'SHOWER(S) OR INTERMITTENT PRECIPITATION',
                '181': 'RAIN SHOWER(S) OR INTERMITTENT RAIN, SLIGHT',
                '182': 'RAIN SHOWER(S) OR INTERMITTENT RAIN, MODERATE',
                '183': 'RAIN SHOWER(S) OR INTERMITTENT RAIN, HEAVY',
                '184': 'RAIN SHOWER(S) OR INTERMITTENT RAIN, VIOLENT',
                '185': 'SNOW SHOWER(S) OR INTERMITTENT SNOW, SLIGHT',
                '186': 'SNOW SHOWER(S) OR INTERMITTENT SNOW, MODERATE',
                '187': 'SNOW SHOWER(S) OR INTERMITTENT SNOW, HEAVY',
                '188': 'RESERVED',
                '189': 'HAIL', 
                '190': 'THUNDERSTORM',
                '191': 'THUNDERSTORM, SLIGHT OR MODERATE, WITH NO PRECIPITATION',
                '192': 'THUNDERSTORM, SLIGHT OR MODERATE, WITH RAIN SHOWERS AND/OR SNOW SHOWERS',
                '193': 'THUNDERSTORM, SLIGHT OR MODERATE, WITH HAIL',
                '194': 'THUNDERSTORM, HEAVY, WITH NO PRECIPITATION',
                '195': 'THUNDERSTORM, HEAVY, WITH RAIN SHOWERS AND/OR SNOW SHOWERS',
                '196': 'THUNDERSTORM, HEAVY, WITH HAIL',
                '199': 'TORNADO',
                '204': 'VOLCANIC ASH SUSPENDED IN THE AIR ALOFT',
                '205': 'NOT USED',
                '206': 'THICK DUST HAZE, VISIBILITY LESS THAN 1 KM',   
                '207': 'BLOWING SPRAY AT THE STATION',
                '208': 'DRIFTING DUST (SAND)', 
                '209': 'WALL OF DUST OR SAND IN DISTANCE (LIKE HABOOB)',
                '210': 'SNOW HAZE',
                '211': 'WHITEOUT' ,
                '212': 'NOT USED',
                '213': 'LIGHTNING, CLOUD TO SURFACE',
                '217': 'DRY THUNDERSTORM',
                '218': 'NOT USED',
                '219': 'TORNADO CLOUD (DESTRUCTIVE) AT OR WITHIN SIGHT OF THE STATION DURING PRECEDING HOUR OR AT THE TIME OF OBSERVATION',
                '220': 'DEPOSITION OF VOLCANIC ASH',
                '221': 'DEPOSITION OF DUST OR SAND',
                '222': 'DEPOSITION OF DEW',
                '223': 'DEPOSITION OF WET SNOW',
                '224': 'DEPOSITION OF SOFT RIME', 
                '225': 'DEPOSITION OF HARD RIME',
                '226': 'DEPOSITION OF HOAR FROST',
                '227': 'DEPOSITION OF GLAZE',
                '228': 'DEPOSITION OF ICE CRUST (ICE SLICK)',
                '229': 'NOT USED',
                '230': 'DUSTSTORM OR SANDSTORM WITH TEMPERATURE BELOW 0 °C',
                '239': 'BLOWING SNOW, IMPOSSIBLE TO DETERMINE WHETHER SNOW IS FALLING OR NOT',
                '240': 'NOT USED',
                '241': 'FOG ON SEA',
                '242': 'FOG IN VALLEYS',
                '243': 'ARCTIC OR ANTARCTIC SEA SMOKE',
                '244': 'STEAM FOG (SEA, LAKE OR RIVER)',
                '245': 'STEAM LOG (LAND)',
                '246': 'FOG OVER ICE OR SNOW COVER',
                '247': 'DENSE FOG, VISIBILITY 60-90 M',
                '248': 'DENSE FOG, VISIBILITY 30-60 M',
                '249': 'DENSE FOG, VISIBILITY LESS THAN 30 M',
                '250': 'DRIZZLE, RATE OF FALL - LESS THAN 0.10 MM H-1',
                '251': 'DRIZZLE, RATE OF FALL - 0.10-0.19 MM H-1',
                '252': 'DRIZZLE, RATE OF FALL - 0.20-0.39 MM H-1',
                '253': 'DRIZZLE, RATE OF FALL - 0.40-0.79 MM H-1',
                '254': 'DRIZZLE, RATE OF FALL - 0.80-1.59 MM H-1',
                '255': 'DRIZZLE, RATE OF FALL - 1.60-3.19 MM H-1',
                '256': 'DRIZZLE, RATE OF FALL - 3.20-6.39 MM H-1',
                '257': 'DRIZZLE, RATE OF FALL - 6.4 MM H-1 OR MORE',
                '258': 'NOT USED',
                '259': 'DRIZZLE AND SNOW',
                '260': 'RAIN, RATE OF FALL - LESS THAN 1.0 MM H-1',
                '261': 'RAIN, RATE OF FALL - 1.0-1.9 MM H-1',
                '262': 'RAIN, RATE OF FALL - 2.0-3.9 MM H-1',
                '263': 'RAIN, RATE OF FALL - 4.0-7.9 MM H-1',
                '264': 'RAIN, RATE OF FALL - 8.0-15.9 MM H-1',
                '265': 'RAIN, RATE OF FALL - 16.0-31.9 MM H-1',
                '266': 'RAIN, RATE OF FALL - 32.0-63.9 MM H-1',
                '267': 'RAIN, RATE OF FALL - 64.0 MM H-1 OR MORE',
                '270': 'SNOW, RATE OF FALL - LESS THAN 1.0 CM H-1',
                '271': 'SNOW, RATE OF FALL - 1.0-1.9 CM H-1',
                '272': 'SNOW, RATE OF FALL - 2.0-3.9 CM H-1',
                '273': 'SNOW, RATE OF FALL - 4.0-7.9 CM H-1',
                '274': 'SNOW, RATE OF FALL - 8.0-15.9 CM H-1',
                '275': 'SNOW, RATE OF FALL - 16.0-31.9 CM H-1',
                '276': 'SNOW, RATE OF FALL - 32.0-63.9 CM H-1',
                '277': 'SNOW, RATE OF FALL - 64.0 CM H-1 OR MORE',
                '278': 'SNOW OR ICE CRYSTAL PRECIPITATION FROM A CLEAR SKY',
                '279': 'WET SNOW, FREEZING ON CONTACT',
                '280': 'PRECIPITATION OF RAIN',
                '281': 'PRECIPITATION OF RAIN, FREEZING',
                '282': 'PRECIPITATION OF RAIN AND SNOW MIXED',
                '283': 'PRECIPITATION OF SNOW',
                '284': 'PRECIPITATION OF SNOW PELLETS OR SMALL HALL',
                '285': 'PRECIPITATION OF SNOW PELLETS OR SMALL HAIL, WITH RAIN',
                '286': 'PRECIPITATION OF SNOW PELLETS OR SMALL HAIL, WITH RAIN AND SNOW MIXED',
                '287': 'PRECIPITATION OF SNOW PELLETS OR SMALL HAIL, WITH SNOW',
                '288': 'PRECIPITATION OF HAIL',
                '289': 'PRECIPITATION OF HAIL, WITH RAIN',
                '290': 'PRECIPITATION OF HALL, WITH RAIN AND SNOW MIXED',
                '291': 'PRECIPITATION OF HAIL, WITH SNOW',
                '292': 'SHOWER(S) OR THUNDERSTORM OVER SEA',
                '293': 'SHOWER(S) OR THUNDERSTORM OVER MOUNTAINS',
                '508': 'NO SIGNIFICANT PHENOMENON TO REPORT, PRESENT AND PAST WEATHER OMITTED',
                '509': 'NO OBSERVATION, DATA NOT AVAILABLE, PRESENT AND PAST WEATHER OMITTED',
                '510': 'PRESENT AND PAST WEATHER MISSING, BUT EXPECTED',
                '511': 'MISSING VALUE'               
    }
    flag_meaning = []
    for i in values:
        pulling = present_weather.get(i)
        if pulling != None:
            flag_meaning.append(pulling.lower().replace(' ', '_').replace('(',"").replace(')',''))
        else:
            print(i, 'is not in this table')
            continue
    flag_meaning = flag_meaning
    return (' '.join(flag_meaning))    

def table_020004(values):
    past_weather1 = {'0': 'CLOUD COVERING 1/2 OR LESS OF THE SKY THROUGHOUT THE APPROPRIATE PERIOD',
                     '1': 'CLOUD COVERING MORE THAN 1/2 OF THE SKY DURING PART OF THE APPROPRIATE PERIOD AND COVERING 1/2 OR LESS DURING PART OF THE PERIOD',
                     '2': 'CLOUD COVERING MORE THAN 1/2 OF THE SKY THROUGHOUT THE APPROPRIATE PERIOD',
                     '3': 'SANDSTORM, DUSTSTORM OR BLOWING SNOW',
                     '4': 'FOG OR ICE FOG OR THICK HAZE',
                     '5': 'DRIZZLE',
                     '6': 'RAIN',
                     '7': 'SNOW, OR RAIN AND SNOW MIXED',
                     '8': 'SHOWER(S)',
                     '9': 'THUNDERSTORM(S) WITH OR WITHOUT PRECIPITATION',
                     '10': 'NO SIGNIFICANT WEATHER OBSERVED',
                     '11': 'VISIBILITY REDUCED',
                     '12': 'BLOWING PHENOMENA, VISIBILITY REDUCED',
                     '13': 'FOG',
                     '14': 'PRECIPITATION',
                     '15': 'DRIZZLE',
                     '16': 'RAIN',
                     '17': 'SNOW OR ICE PELLETS',
                     '18': 'SHOWERS OR INTERMITTENT PRECIPITATION',
                     '19': 'THUNDERSTORM',
                     '31': 'MISSING VALUE'       
    }
    flag_meaning = []
    for i in values:
        pulling = past_weather1.get(i)
        if pulling != None:
            flag_meaning.append(pulling.lower().replace(' ', '_').replace('(',"").replace(')',''))
        else:
            print(i, 'is not in this table')
            continue
    flag_meaning = flag_meaning
    return (' '.join(flag_meaning)) 

def table_002001(values):
    stationtype = {'0': 'AUTOMATIC',
                   '1': 'MANNED',
                   '2': 'HYBRID: BOTH MANNED AND AUTOMATIC',
                   '3': 'MISSING VALUE'}
    flag_meaning = []
    for i in values:
        pulling = stationtype.get(i)
        if pulling != None:
            flag_meaning.append(pulling.lower().replace(' ', '_').replace('(',"").replace(')',''))
        else:
            print(i, 'is not in this table')
            continue
    flag_meaning = flag_meaning
    return (' '.join(flag_meaning))     


def table_010063(values):
    char_of_pressure = {'0':'INCREASING, THEN DECREASING; ATMOSPHERIC PRESSURE THE SAME OR HIGHER THAN THREE HOURS AGO',
                        '1': 'INCREASING, THEN STEADY; OR INCREASING, THEN INCREASING MORE SLOWLY',
                        '2': 'INCREASING (STEADILY OR UNSTEADILY)',
                        '3': 'DECREASING OR STEADY, THEN INCREASING; OR INCREASING, THEN INCREASING MORE RAPIDLY',
                        '4': 'STEADY; ATMOSPHERIC PRESSURE THE SAME AS THREE HOURS AGO',
                        '5': 'DECREASING, THEN INCREASING; ATMOSPHERIC PRESSURE THE SAME OR LOWER THAN THREE HOURS AGO',
                        '6': 'DECREASING, THEN STEADY; OR DECREASING, THEN DECREASING MORE SLOWLY',
                        '7': 'DECREASING (STEADILY OR UNSTEADILY)',
                        '8': 'STEADY OR INCREASING, THEN DECREASING; OR DECREASING, THEN DECREASING MORE RAPIDLY',
                        '15': 'MISSING VALUE',
                        }
    flag_meaning = []
    for i in values:
        pulling = char_of_pressure.get(i)
        if pulling != None:
            flag_meaning.append(pulling.lower().replace(' ', '_').replace('(',"").replace(')',''))
        else:
            print(i, 'is not in this table')
            continue
    flag_meaning = flag_meaning
    return (' '.join(flag_meaning))             

def table_020005(values):
    past_weather2 = {'0': 'CLOUD COVERING 1/2 OR LESS OF THE SKY THROUGHOUT THE APPROPRIATE PERIOD',
                     '1': 'CLOUD COVERING MORE THAN 1/2 OF THE SKY DURING PART OF THE APPROPRIATE PERIOD AND COVERING 1/2 OR LESS DURING PART OF THE PERIOD',
                     '2': 'CLOUD COVERING MORE THAN 1/2 OF THE SKY THROUGHOUT THE APPROPRIATE PERIOD',
                     '3': 'SANDSTORM, DUSTSTORM OR BLOWING SNOW',
                     '4': 'FOG OR ICE FOG OR THICK HAZE',
                     '5': 'DRIZZLE',
                     '6': 'RAIN',
                     '7': 'SNOW, OR RAIN AND SNOW MIXED',
                     '8': 'SHOWER(S)',
                     '9': 'THUNDERSTORM(S) WITH OR WITHOUT PRECIPITATION',
                     '10': 'NO SIGNIFICANT WEATHER OBSERVED',
                     '11': 'VISIBILITY REDUCED',
                     '12': 'BLOWING PHENOMENA, VISIBILITY REDUCED',
                     '13': 'FOG',
                     '14': 'PRECIPITATION',
                     '15': 'DRIZZLE',
                     '16': 'RAIN',
                     '17': 'SNOW OR ICE PELLETS',
                     '18': 'SHOWERS OR INTERMITTENT PRECIPITATION',
                     '19': 'THUNDERSTORM',
                     '31': 'MISSING VALUE'       
    }
    flag_meaning = []
    for i in values:
        pulling = past_weather2.get(i)
        if pulling != None:
            flag_meaning.append(pulling.lower().replace(' ', '_').replace('(',"").replace(')',''))
        else:
            print(i, 'is not in this table')
            continue
    flag_meaning = flag_meaning
    return (' '.join(flag_meaning)) 
        
def table_008021(values):
    time_significance = {'0': 'RESERVED',
                     '1': 'TIME SERIES',
                     '2': 'TIME AVERAGED',
                     '3': 'ACCUMULATED',
                     '4': 'FORECAST',
                     '5': 'FORECAST TIME SERIES',
                     '6': 'FORECAST TIME AVERAGED',
                     '7': 'FORECAST ACCUMULATED',
                     '8': 'ENSEMBLE MEAN',
                     '9': 'ENSEMBLE MEAN TIME SERIES',
                     '10': 'ENSEMBLE MEAN TIME AVERAGED',
                     '11': 'ENSEMBLE MEAN ACCUMULATED',
                     '12': 'ENSEMBLE MEAN FORECAST',
                     '13': 'ENSEMBLE MEAN FORECAST TIME SERIES',
                     '14': 'ENSEMBLE MEAN FORECAST TIME AVERAGED',
                     '15': 'ENSEMBLE MEAN FORECAST ACCUMULATED',
                     '16': 'ANALYSIS',
                     '17': 'START OF PHENOMENON',
                     '18': 'RADIOSONDE LAUNCH TIME',
                     '19': 'START OF ORBIT',
                     '20': 'END OF ORBIT',
                     '21': 'TIME OF ASCENDING NODE',
                     '22': 'TIME OF OCCURRENCE OF WIND SHIFT',
                     '23': 'MONITORING PERIOD',
                     '24': 'AGREED TIME LIMIT FOR REPORT RECEPTION',
                     '25': 'NOMINAL REPORTING TIME',
                     '26': 'TIME OF LAST KNOWN POSITION',
                     '27': 'FIRST GUESS',
                     '28': 'START OF SCAN',
                     '29': 'END OF SCAN OR TIME OF ENDING',
                     '30': 'TIME OF OCCURRENCE',
                     '31': 'MISSING VALUE'       
    }
    flag_meaning = []
    for i in values:
        pulling = time_significance.get(i)
        if pulling != None:
            flag_meaning.append(pulling.lower().replace(' ', '_').replace('(',"").replace(')',''))
        else:
            print(i, 'is not in this table')
            continue
    flag_meaning = flag_meaning
    return (' '.join(flag_meaning)) 

def table_020062(values):
    state_of_the_ground = {
        '0': 'SURFACE OF GROUND DRY (WITHOUT CRACKS AND NO APPRECIABLE AMOUNT OF DUST OR LOOSE SAND)',
        '1': 'SURFACE OF GROUND MOIST',
        '2': 'SURFACE OF GROUND WET (STANDING WATER IN SMALL OR LARGE POOLS ON SURFACE)',
        '3': 'FLOODED',
        '4': 'SURFACE OF GROUND FROZEN',
        '5': 'GLAZE ON GROUND',
        '6': 'LOOSE DRY DUST OR SAND NOT COVERING GROUND COMPLETELY',
        '7': 'THIN COVER OF LOOSE DRY DUST OR SAND COVERING GROUND COMPLETELY',
        '8': 'MODERATE OR THICK COVER OF LOOSE DRY DUST OR SAND COVERING GROUND COMPLETELY',
        '9': 'EXTREMELY DRY WITH CRACKS',
        '10': 'GROUND PREDOMINANTLY COVERED BY ICE',
        '11': 'COMPACT OR WET SNOW (WITH OR WITHOUT ICE) COVERING LESS THAN ONE HALF OF THE GROUND',
        '12': 'COMPACT OR WET SNOW (WITH OR WITHOUT ICE) COVERING AT LEAST ONE HALF OF THE GROUND BUT GROUND NOT COMPLETELY COVERED',
        '13': 'EVEN LAYER OF COMPACT OR WET SNOW COVERING GROUND COMPLETELY',
        '14': 'UNEVEN LAYER OF COMPACT OR WET SNOW COVERING GROUND COMPLETELY',
        '15': 'LOOSE DRY SNOW COVERING LESS THAN ONE HALF OF THE GROUND',
        '16': 'LOOSE DRY SNOW COVERING AT LEAST ONE HALF OF THE GROUND BUT GROUND NOT COMPLETELY COVERED',
        '17': 'EVEN LAYER OF LOOSE DRY SNOW COVERING GROUND COMPLETELY',
        '18': 'UNEVEN LAYER OF LOOSE DRY SNOW COVERING GROUND COMPLETELY',
        '19': 'SNOW COVERING GROUND COMPLETELY; DEEP DRIFTS',
        '31': 'MISSING VALUE'
        }
    flag_meaning = []
    for i in values:
        pulling = state_of_the_ground.get(i)
        if pulling != None:
            flag_meaning.append(pulling.lower().replace(' (', '(').replace(' ', '_').replace('(',"").replace(')',''))
        else:
            print(i, 'is not in this table')
            continue
    flag_meaning = flag_meaning
    return (' '.join(flag_meaning)) 