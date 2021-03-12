import logging
from datetime import datetime
from io import BytesIO
from socket import gethostname, gethostbyname

from smb.SMBConnection import SMBConnection
from smb.smb_structs import OperationFailure

import secrets

"""
This module contains all functions for work with Server Message Block protocol, 
which is used to storing files from Linux server (script hosting) to Win server (bigshare)"""

client_machine_name = gethostname()  # 'qlik-script'
server_ip = gethostbyname('bigshare')  # '192.168.219.57'

userID = secrets.server_userID
password = secrets.server_password
server_name = secrets.server_name
domain_name = secrets.domain_name
port = secrets.port


def isdir(path_middle, directory) -> bool:
    """
    Checks if there's directory "//bigshare/Выгрузки ТФ/<path_middle>/<directory>"
    :param path_middle: part of full path (str)
    :param directory: name of checking directory at the end of full path (str/int)
    :return: True, if the directory exists, otherwise False
    Example call:
    isdir(path_middle='Выгрузки My_TK/2020', directory='11. November')
    """
    directory = str(directory)
    conn = SMBConnection(userID, password, client_machine_name, server_name,
                         domain=domain_name, use_ntlm_v2=True, is_direct_tcp=True)
    conn.connect(server_ip, port)
    year_dir = [file for file in conn.listPath('Выгрузки ТФ', path_middle)
            if file.filename == directory and file.isDirectory]
    conn.close()
    return bool(year_dir)


def mkdir(path_middle, directory):
    """
    Tries to create <directory> at path end like: //bigshare/Выгрузки ТФ/<path_middle>/<directory>
    Rewriting not allowed. If the directory already exists createDirectory throws OSError.
    :param path_middle: part of full path (str)
    :param directory: name of the creating directory at the end of full path (str/int)
    :return: None
    Example call:
    mkdir(path_middle='Выгрузки My_TK', directory='2022')
    """
    directory = str(directory)
    conn = SMBConnection(userID, password, client_machine_name, server_name, domain=domain_name, use_ntlm_v2=True,
                     is_direct_tcp=True)
    conn.connect(server_ip, port)
    try:
        conn.createDirectory('Выгрузки ТФ', f'{path_middle}/{directory}')
    except OperationFailure:
        conn.close()
        raise OSError
    conn.close()


def set_bigshare_dir_smb(date):
    """
    Check if proper folder exists. If not - creates it.
    Returns path with folder for inputed date
    Exapmple call:
    set_bigshare_dir_smb('2028-01-01')
    will return: 'Выгрузки My_TK/2028/01. January'

    :param: date to save in 'YYYY-MM-DD' format (str)
    :return: path for saving
    """
    logger = logging.getLogger(__name__)
    date = datetime.strptime(date, '%Y-%m-%d').date()  # str -> datetime.date
    path_middle = 'Выгрузки My_TK'  # fullpath is '//bigshare/Выгрузки ТФ/Выгрузки My_TK/{date.year}'

    if not isdir(path_middle=path_middle, directory=date.year):  # if year_directory not exists
        try:
            mkdir(path_middle=path_middle, directory=date.year)  # create it
        except OSError:
            print(f"Creation of the directory 'bigshare/Выгрузки ТФ/{path_middle}/{date.year}' failed")
            logger.error(f"Creation of the directory 'bigshare/Выгрузки ТФ/{path_middle}/{date.year}' failed")
        else:
            logger.info(f"Successfully created the directory 'bigshare/Выгрузки ТФ/{path_middle}/{date.year}'")
            print(f"Successfully created the directory 'bigshare/Выгрузки ТФ/{path_middle}/{date.year}'")
    month = date.strftime('%m') + '. ' + date.strftime('%B')
    path_middle += '/' + str(date.year)
    if not isdir(path_middle=path_middle, directory=month):  # if month_directory not exists
        try:
            mkdir(path_middle=path_middle, directory=month)  # create it
        except OSError:
            print(f"Creation of the directory 'bigshare/Выгрузки ТФ/{path_middle}/{month}' failed")
            logger.error(f"Creation of the directory 'bigshare/Выгрузки ТФ/{path_middle}/{month}' failed")
        else:
            print(f"Successfully created the directory 'bigshare/Выгрузки ТФ/{path_middle}/{month}'")
            logger.info(f"Successfully created the directory 'bigshare/Выгрузки ТФ/{path_middle}/{month}' failed'")
    return path_middle + '/' + month


def store_dataframe(df, path, date, name):
    """
    Stores Pandas dataframe into //bigshare/Выгрузки ТФ/<path>
    in csv format with ; separator and provided name. If file already exists - it overwrites it.
    :param df: Pandas DataFrame for saving
    :param path: path with directory at the end in string format, like 'Выгрузки My_TK/2028/01. January'
    :param date: date for filename
    :param name: name ending, in fact it's loadout type, like '2021-02-01_заказы'
    :return: None or Exception
    """
    logger = logging.getLogger(__name__)
    conn = SMBConnection(userID, password, client_machine_name, server_name,
                         domain=domain_name, use_ntlm_v2=True, is_direct_tcp=True)
    conn.connect(server_ip, port)
    byte_buffer = BytesIO()
    df.to_csv(byte_buffer, sep=';', index=False)
    byte_buffer.seek(0)

    try:
        conn.storeFile('Выгрузки ТФ', f'{path}/{date}{name}.csv', byte_buffer)
    except Exception as e:
        print(e)
        logger.error(f"File storing caused error:\n{e}")
        conn.close()
    conn.close()
