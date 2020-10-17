import base64
import json
import logging
import os
import re
import zlib
from datetime import datetime, timedelta
from random import shuffle
from traceback import print_exc

import matplotlib.path as mpltPath
import pandas as pd
import requests
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

"""
TK_utils contains data extract functions (server_request and decode_decompress),
data load function (load_to_GDrive), and various utilities (time_transform, set_date, set_interval)
and MANY MOAR
"""


def time_transform(x):
    """
    Transforms time format
    :param x: time in ugly 'hm' string format
    :return: time in fabulous 'hh:mm:00' string format
    """
    if len(x) == 1:
        return '00:0' + x + ':00'
    elif len(x) == 2:
        return '00:' + x + ':00'
    elif len(x) == 3:
        return '0' + x[0] + ':' + x[1:] + ':00'
    elif len(x) == 4:
        return x[:2] + ':' + x[2:] + ':00'
    else:
        raise RuntimeError("Wrong time format received")


def load_to_GDrive(csv_path):
    """Loads the file to the My_TK folder onto my personal Google Drive
    Args: path to loaded csv file
    :return: None"""
    """Link to share my 'My_TK' Google Drive folder:
     https://drive.google.com/drive/folders/1DMJArpNF8k2qC3h8Cqc4qtTGUfL1z-y4?usp=sharing
     """
    logging.getLogger('googleapiclient.discovery').setLevel(logging.CRITICAL)

    my_TK_folder_id = '1DMJArpNF8k2qC3h8Cqc4qtTGUfL1z-y4'
    file_name = csv_path[5:]
    gauth = GoogleAuth()  # https://stackoverflow.com/questions/24419188/automating-pydrive-verification-process
    gauth.LoadCredentialsFile("mycreds.txt")
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.GetFlow()
        gauth.flow.params.update({'access_type': 'offline'})
        gauth.flow.params.update({'approval_prompt': 'force'})
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile("mycreds.txt")

    drive = GoogleDrive(gauth)
    # Update: deletes file if it's already exists
    file_list = drive.ListFile({'q': f"'{my_TK_folder_id}' in parents and trashed=false"}).GetList()
    for existing_file in file_list:
        if existing_file['title'] == file_name:
            existing_file.Delete()

    file = drive.CreateFile({'parents': [{'id': my_TK_folder_id}]})
    file.SetContentFile(csv_path)
    file['title'] = file_name  # cuts 'data/'
    file.Upload()
    logger = logging.getLogger(__name__)
    logger.info(file_name + " is loaded to Google Drive")
    # TODO: maybe change file name for data pipeline?


def server_request(api_url_tail, params):
    """
    Request server via REST API
    :param api_url_tail: tail from CONSTANTS.py to form whole URL like 'http://' + ip + api_url_tail
    :param params: REST API params(type_query, login, pass, date, lang)
    :return: content of the request in JSON format (some fields are coded)
    """
    logger = logging.getLogger(__name__)
    # Get request to server
    ip_list = ['192.168.221.30:81', '192.168.212.30:81', '192.168.218.30:81']
    res = False
    while ip_list:
        shuffle(ip_list)
        ip = ip_list.pop()
        api_url = 'http://' + ip + api_url_tail
        res = requests.get(api_url, params=params)
        if res.status_code == 200:
            break
    if res.status_code != 200:
        print("Status_code from all IP's is not 200")
        logger.critical(f"Status_code from all IP's is not 200, the last is {res.status_code}")
        print(f"status_code from last used IP: {res.status_code}")
        raise RuntimeError("Server gives not ok respond")
    else:
        res_content = json.loads(res.content)  # res.content - "binary" data, not html-formatted
        # print(f"Response from call {params['type_query']} contains fields: ")
        # for key, val in res_content.items():
        #     print(key, end='\t')
        # print('')
        print(f"{params['type_query']}...")
        if res_content['status'] != "1":
            print('API respond.content["status"] is not "1", it is ', res_content['status'])
            logger.error('API respond.content["status"] is not "1", it is ', res_content['status'])
            raise RuntimeError("API gives not ok respond")
    return res_content


def decode_decompress(data, backup_name=None, date=None):
    """
    Decodes 'data' part of respond and saves it locally as JSON
    :param data: 'data' part of the server respond
    :param backup_name: just for filename to save
    :param date: and this is too just for filename
    :return: decoded Python list
    """
    # Takes 'data' part of the server respond
    encoded_data_bytes = data.encode('UTF-8')  # string into a bytes-like object
    decoded_data_bytes = base64.b64decode(encoded_data_bytes)  # bytes-like object base64 decoded
    final_data_bytes = zlib.decompress(decoded_data_bytes)  # bytes-like object zlib decompressed
    final_data_json = final_data_bytes.decode('utf-8')  # encoded string to decoded plain string with JSON data
    if date is not None:
        with open(f"backup/{backup_name}_{date}.JSON", 'wb') as f:
            f.write(final_data_json.encode('utf8'))
    final_data = json.loads(final_data_json)  # parse JSON to python (list)
    return final_data


def set_date():
    """
    Primitive user interface. Requests user to input date for loading
    :return: date in 'YYYY-MM-DD' formal
    """
    while True:
        print('Pls input date in format "YYYY-MM-DD" (w/o quotes)\nor press Enter for yesterday: 2020-', end="")
        load_date = input()  # just for 2020
        pattern = r"(0[1-9]|1[0-2])-(0[1-9]|[1-2]\d|3[0-1])"
        # print('Pls input date in format "YYYY-MM-DD" (w/o quotes)\nor press Enter for default value: ', end="")
        # load_date = input()
        # pattern = r"202\d-(0[1-9]|1[0-2])-(0[1-9]|[1-2]\d|3[0-1])"
        match = re.match(pattern, load_date)
        if match is not None:
            break
        if not load_date:
            load_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # default value - yesterday
            print(load_date)
            return load_date
        print("Date format's wrong, lol!")
    # just for 2020
    load_date = '2020-' + load_date
    return load_date


def set_interval():
    """
    User interface for request begin and end dates (included) for load an interval
    :return: list of dated in 'YYYY-MM-DD' format
    """
    print('Start date!', end=' ')
    start_date = set_date()
    print('End date!', end=' ')
    end_date = set_date()
    # Create date range
    dates = pd.date_range(start_date, end_date, freq='D').tolist()
    date_list = []
    for date_i in dates:
        date_list.append(date_i.strftime("%Y-%m-%d"))
    return date_list


def set_bigshare_dir(date):
    """
    Check if proper folder exists. If not - creates it.
    :param: date to save
    :return: path for saving
    """
    logger = logging.getLogger(__name__)
    date = datetime.strptime(date, '%Y-%m-%d').date()  # str -> datetime.date
    access_rights = 0o755  # readable and accessible by all users, and write access by only the owner

    path = f'//bigshare/Выгрузки ТФ/Выгрузки My_TK/{date.year}'
    if not os.path.isdir(path):  # if year_directory not exists
        try:
            os.mkdir(path, access_rights)  # create it
        except OSError:
            print(f"Creation of the directory '{path}' failed")
            logger.error(f"Creation of the directory '{path}' failed")
        else:
            logger.info(f"Successfully created the directory '{path}'")
    month = date.strftime('%m') + '. ' + date.strftime('%B')
    path += '/' + month
    if not os.path.isdir(path):  # if month_directory not exists
        try:
            os.mkdir(path, access_rights)  # create it
        except OSError:
            print(f"Creation of the directory '{path}' failed")
            logger.error(f"Creation of the directory '{path}' failed")
        else:
            logger.info(f"Successfully created the directory '{path}'")
    return path


def decode_way(encoded: str) -> list:
    """
    Decodes 'compressed_boundary' column in get_qlick_geo_zones decoded & decompressed 'data' respond
    :param encoded: 'compressed_boundary' string from 'data'
    :return: list of dicts like [{X: lat1, Y: lng1}, {X: lat2, Y: lng2}]
    """
    array = list()
    try:
        precision = 10 ** (-6)
        index = lat = lng = 0
        while index < len(encoded):
            b = shift = result = 0
            while True:  # first inner do-while
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            dlat = float(~(result >> 1)) if (result & 1) == 1 else float(result >> 1)
            lat += dlat
            shift = result = 0
            while True:  # second inner do-while
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            dlng = float(~(result >> 1)) if (result & 1) == 1 else float(result >> 1)
            lng += dlng

            it = dict()
            it['X'] = round(lat * precision, 6)
            it['Y'] = round(lng * precision, 6)
            array.append(it)

    except Exception as e:
        print_exc()
        array = []
    return array


def calc_barycenter(list_of_dicts):
    """DEPRECATED"""
    x_sum = y_sum = 0
    for point in list_of_dicts:
        x_sum += point['X']
        y_sum += point['Y']
    result = {'X': round(x_sum / len(list_of_dicts), 6), 'Y': round(y_sum / len(list_of_dicts), 6)}
    return result


def is_in_polygon(X, Y, polygon):
    """
    Calculates if point is inside polygon.
    :X: x coordinate of the point
    :Y: y coordinate of the point
    :polygon: list of dicts with a points of vertices of the polygon
    :return: True / False
    """
    max_polygon_X = max(point['X'] for point in polygon)
    min_polygon_X = min(point['X'] for point in polygon)
    max_polygon_Y = max(point['Y'] for point in polygon)
    min_polygon_Y = min(point['Y'] for point in polygon)
    if X < min_polygon_X or X > max_polygon_X or Y < min_polygon_Y or Y > max_polygon_Y:
        return False
    flat_polygon = [list(dct.values()) for dct in polygon]  # list of dicts -> list of lists
    path = mpltPath.Path(flat_polygon)
    return path.contains_point([X, Y])


def manual_logging_timedelta(load_start_time):
    """
    Prints elapsed loading time to log.
    :param load_start_time: time in 'datetime.datetime' format
    :return: None
    """
    logger = logging.getLogger(__name__)
    load_finish_time = datetime.now()
    finish_start_timedelta = load_finish_time - load_start_time
    hours = int(finish_start_timedelta.total_seconds()//3600)
    minutes = int(finish_start_timedelta.total_seconds()//60) % 60
    seconds = int(finish_start_timedelta.total_seconds() % 60)
    logger.info(f"Loading duration: {hours} hours, {minutes} minutes, {seconds} sec.")


def is_bit_setted(x: int, n_of_bit: int = 0):
    """
    Checks if n-th bit is setted (==1) or not(==0) in number x. Counts n-th from right to left!
    :param x: integer to check bites
    :param n_of_bit: number of the checking bit
    :return: True if bit is set, otherwise - False
    """
    if x & (1 << n_of_bit):
        return True
    else:
        return False
