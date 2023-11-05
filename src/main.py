from maps import *
from shapely.geometry import shape, Point
import psycopg2
import json
from rtree import index
import requests
import zipfile
import io
import logging


'''
for db creds we would of course want to use a service or somthing for getting the credentials 
for simplicity just hard coding them here
'''
db_creds = {
    'host': '0.0.0.0',
    'port': 5432,
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'mysecretpassword'
}
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """
    since we are not going to do any processing in the database only simple inserts of a batch
    of records we can just use a simple jdbc connection, no pool or ORM or anything
    """
    with psycopg2.connect(**db_creds) as conn:
        cur = conn.cursor()

        """
        setup table for ingested records if it doesn't exist
        just stuck it here for simplicity
        """
        with open('create_ingested_records.sql', 'r') as f:
            logger.info('creating ingested_records table if not exists')
            '''for demo just droping the table and recreating so no primary key errors
               would need to handle them in the data or in the insert'''
            cur.execute('drop table if exists ingested_records;')
            conn.commit()
            cur.execute(f.read())

        """
        get raw input data
        """
        logger.info('getting raw data')
        raw_data = get_data()
        input_records = make_input_record_dict(raw_data)

        """
        get us county geometry data
        make a spatial index for the county geometries
        """
        with open('counties.geojson', 'r') as f:
            us_geom_data = json.load(f)
        counties = us_geom_data['features']
        counties_idx = get_county_index(counties)

        logger.info(f'ingesting {len(input_records)} records')
        output_insert_list = []
        non_us_count = 0
        for raw_record_dict in input_records[:]:
            '''first check if the record point against the index'''
            idx_intersects = list(counties_idx.intersection(raw_record_dict['geom'].bounds))
            if not idx_intersects:
                '''we could put these (or some of them) into a rejected table if need be'''
                # print(f'{raw_record_dict["GLOBALEVENTID"]},{raw_record_dict["ActionGeo_CountryCode"]}: no intersection, not is US')
                non_us_count += 1
            else:
                ''''if we have intersections with the index we can filter down to those counties and check for intersection'''
                county_idx_intersects = list(counties[i] for i in idx_intersects)
                county_intersects = list(filter(lambda c: shape(c['geometry']).contains(raw_record_dict['geom']), county_idx_intersects))
                if not county_intersects:
                    '''still not in the US'''
                    non_us_count += 1
                elif len(county_intersects) > 1:
                    '''record is in multiple counties, bad geom someplace we may want to raise'''
                    fps = list(i['properties']['STATE'] + i['properties']['COUNTY'] for i in county_intersects)
                    logger.warning(f'WARNING: potential overlap in county geometry between {fps}')
                else:
                    '''we found the county the record is in'''
                    # print(f'{raw_record_dict["GLOBALEVENTID"]}: in  {c_intersects[0]["properties"]["NAME"]}, {c_intersects[0]["properties"]["STATE"]}')
                    output_insert_list.append(make_insert_row(raw_record_dict, county_intersects[0]))

        logger.info(f'found {len(output_insert_list)} records in the US')
        logger.info(f'found {non_us_count} records not in the US')

        '''insert records into ingested table'''
        if len(output_insert_list) == 0:
            logger.info('no records to insert')
        else:
            sql = f"""insert into ingested_records (globaleventid, src_sqldate, src_eventcode, event, src_eventbasecode,
                           event_base, src_eventrootcode, event_root, src_actiongeo_fullname, src_actiongeo_countrycode,
                            county_fips, county_name,
                            src_actiongeo_lat, src_actiongeo_long, src_dateadded, src_sourceurl, geom)
                      values {','.join(output_insert_list)}"""
            # print(sql)
            logger.info(f'inserting {len(output_insert_list)} records into ingested_records table')
            cur.execute(sql)
            logger.info('db update complete')


'''
started this for what we might do for handling the conversion of date columns in the raw data
'''
# def convert_to_datetime(datetime_string):
#     format = 'yyyyMMdd'
#     try:
#         datetime = datetime.datetime.strptime(input, format)
#         return datetime.datetime
#     except:
#         return None


def get_county_index(features):
    idx = index.Index()
    for k, f in enumerate(features):
        geom = shape(f['geometry'])
        bbox = geom.bounds
        idx.insert(k, bbox)

    return idx


def get_data():
    """
    this function will download the data from the remote site
    for simplicy just a simple get request, probably need somthing better here
    """
    url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    return_data = None
    
    response = requests.get(url)
    file_url = None
    if response.status_code == 200:
        for i in response.content.decode('utf-8').split('\n'):
            if '.export.CSV' in i:
                file_url = i.split(' ')[-1]     
    #print(file_url)
    
    response2 = requests.get(file_url)
    if response2.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response2.content), 'r') as zipped_file:
            file_info = zipped_file.infolist()[0]
            byte_string = zipped_file.read(file_info.filename)
            decoded_str = byte_string.decode('utf-8')
            return_data = decoded_str   
    
    if not return_data:
        raise Exception('ERROR: failed to get data from remote site')

    return return_data


def make_input_record_dict(raw_data):
    """
    this function is going to read the raw datae,
    get the columns of data we want,
    look up the event codes from the provided maps, setting the values to "unknown" if the code can't be mapped
    and return a list of dicts
    """
    return_list = []
    l_num = 0
    for l in raw_data.split('\n')[:]:
        row = l.split('\t')
        try:
            lat = float(row[56])
            lng = float(row[57])
            return_row_dict = {
                'GLOBALEVENTID': row[0],
                'SQLDATE': row[1],
                'EventCode': row[26],
                'EventBaseCode': row[27],
                'EventRootCode': row[28],
                'ActionGeo_FullName': row[52],
                'ActionGeo_CountryCode': row[53],
                'ActionGeo_Lat': row[56],
                'ActionGeo_Long': row[57],
                'DATEADDED': row[59],
                'SOURCEURL': row[60],
                'geom': Point([lng, lat])
            }
        except ValueError:
            # print(f'ValueError: bad geometry for GLOBALEVENTID {row[0]}: {row[56]}, {row[57]}')
            continue
        except IndexError:
            logger.warning(f'bad row in raw data: line: {l_num}, {row}')
            continue

        '''
        lookup events by code, set any bad codes or unknown codes to "unknown"
        might want to reject these records? not sure
        '''
        return_row_dict['event'] = get_mapped_value(return_row_dict['EventCode'], event_codes)
        return_row_dict['event_base'] = get_mapped_value(return_row_dict['EventBaseCode'], event_base_codes)
        return_row_dict['event_root'] = get_mapped_value(return_row_dict['EventRootCode'], event_root_codes)
        return_list.append(return_row_dict)
        l_num += 1

    return return_list


def get_mapped_value(raw_value, mapping_dict):
    """
    this function is going to look up the raw value in the provided mapping dict,
    returning "unknown" if not found
    """
    try:
        return mapping_dict[raw_value]
    except KeyError:
        # print(f'ERROR: no mapping found for {raw_value}')
        return 'unknown'


def make_insert_row(raw_record_dict, county_dict):
    """
    this function is going to take the raw record dict and the county dict
    and return a string that can be used as a "row" in an insert statement for the ingested_records table
    """

    value_list = [raw_record_dict["GLOBALEVENTID"], raw_record_dict["SQLDATE"],
                  raw_record_dict['EventCode'], raw_record_dict['event'],
                  raw_record_dict['EventBaseCode'], raw_record_dict['event_base'],
                  raw_record_dict['EventRootCode'], raw_record_dict['event_root'],
                  raw_record_dict['ActionGeo_FullName'], raw_record_dict['ActionGeo_CountryCode'],
                  county_dict['properties']['STATE'] + county_dict['properties']['COUNTY'], county_dict['properties']['NAME'],
                  raw_record_dict['ActionGeo_Lat'], raw_record_dict['ActionGeo_Long'],
                  raw_record_dict['DATEADDED'], raw_record_dict['SOURCEURL'],
                  raw_record_dict['geom'].wkt]
    formated_row_list = list(map(lambda x: f"'{x.strip()}'", value_list))
    row_string = ','.join(formated_row_list)
    cleaned_row_string = row_string.replace('\n', '')

    return f"({cleaned_row_string})"


if __name__ == '__main__':
    main()
