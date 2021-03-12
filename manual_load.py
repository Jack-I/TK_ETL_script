import logging
from datetime import datetime

import pandas as pd

import SMB_functions as smb_f
import TK_utils as tk_u
import dataframe_transformations as df_t
import renaming_dicts
import secrets

"""
manual_load.py contains load_one_date function, which do what it says.
This module can be executed for manual load one date or date interval.
It also can be imported for scheduling load
"""


def load_one_date(date):
    """Loads only ine particular date onto hard drive (and Google Drive - now disabled).
    \nInputs: selected date in "YYYY-MM-DD" format
    \nOutputs: Nothing"""
    # Pins and unformed, car classes, cities_ids
    logger = logging.getLogger(__name__)
    logger.info(f'...loading "{date}"')
    print(f'...loading "{date}"')
    unf_params = {
        'type_query': 'get_qlick_leads',  # it's name mistake, in fact it loads only unformed orders
        'name': secrets.login,
        'pass': secrets.password,
        'date': date,
        'lang': 'ru'
    }
    unformed_content = tk_u.server_request(secrets.api_url_tail, unf_params)

    # Car classes
    car_classes_df = pd.DataFrame(unformed_content['car_classes'])  # 'car classes' in JSON table
    car_classes_df['id'] = car_classes_df['id'].astype('int64')  # for proper sorting
    car_classes_df.sort_values(by='id', ignore_index=True, inplace=True)
    # car_classes_df.to_csv(r"match_tables/car_classes.csv", sep=';', index=False)

    # Cities ids and timezone corrections
    cities_df = pd.DataFrame(unformed_content['cities_ids'])
    cities_df['id'] = cities_df.id.astype(int)
    cities_df.name.replace(r'г\. ', '', regex=True, inplace=True)
    cities_df['to_local_time_corr'] = cities_df.name.map(renaming_dicts.time_zones)  # just numeric field
    cities_df['to_local_time_corr'] = pd.to_timedelta(cities_df.to_local_time_corr, unit='hour')
    try:
        smb_f.store_dataframe(cities_df, 'Общая база qvd/', date='', name='cities_ids_and_types')
    except PermissionError:
        logger.warning(f'Cities ids file is locked by another user. Rewriting failed.')

    # Options for orders
    options_df = pd.DataFrame(unformed_content['options'])
    options_df = options_df[options_df.id_option < 94]  # Tf options
    options_df = options_df.append({'id_option': 10, 'name': 'Скид.карта'}, ignore_index=True)
    options_df.sort_values(by='id_option', ignore_index=True, inplace=True)

    # Geozones
    geo_params = {
        'type_query': 'get_qlick_geo_zones',
        'name': secrets.login,
        'pass': secrets.password,
        'lang': 'ru'
    }
    geo_content = tk_u.server_request(secrets.api_url_tail, geo_params)
    geo_json = tk_u.decode_decompress(geo_content['data'])
    geo_df = df_t.get_geozones(pd.DataFrame(geo_json), cities_df=cities_df)

    # Unformed orders
    unformed_json = tk_u.decode_decompress(unformed_content['data'], backup_name='unf', date=date)
    df_t.modify_and_save_unformed(pd.DataFrame(unformed_json),
                                  car_classes_df=car_classes_df,
                                  cities_df=cities_df,
                                  geo_df=geo_df,
                                  date=date)
    logger.info(f'{unf_params["type_query"]} has loaded ')

    # Pins
    pin_params = {
        'type_query': 'get_qlick_pins',
        'name': secrets.login,
        'pass': secrets.password,
        'date': date,
        'lang': 'ru'
    }
    pin_content = tk_u.server_request(secrets.api_url_tail, pin_params)
    pin_json = tk_u.decode_decompress(pin_content['data'], backup_name='pins', date=date)
    df_t.modify_and_save_pins(pd.DataFrame(pin_json),
                              car_classes_df=car_classes_df,
                              cities_df=cities_df,
                              geo_df=geo_df,
                              date=date)
    logger.info(f'{pin_params["type_query"]} has loaded ')

    # Orders
    ord_params = {
        'type_query': 'get_qlick_orders',
        'name': secrets.login,
        'pass': secrets.password,
        'date': date,
        'lang': 'ru'
    }
    ord_content = tk_u.server_request(secrets.api_url_tail, ord_params)
    ord_json = tk_u.decode_decompress(ord_content['data'], backup_name='orders', date=date)
    df_t.modify_and_save_orders(pd.DataFrame(ord_json),
                                car_classes_df=car_classes_df,
                                cities_df=cities_df,
                                options_df=options_df,
                                geo_df=geo_df,
                                date=date)
    logger.info(f'{ord_params["type_query"]} has loaded')


if __name__ == '__main__':
    logging.basicConfig(filename='TK_manual.log',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filemode='a',
                        level=logging.INFO)
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    logger = logging.getLogger(__name__)
    print('Hello there!\nTo load one date press Enter.\nTo load an interval enter anything: ', end='')
    select = input()
    if select:
        date_range = tk_u.set_interval()
        load_start_timestamp = datetime.now()
        for date in date_range:
            load_one_date(date)
    else:
        date = tk_u.set_date()
        load_start_timestamp = datetime.now()
        load_one_date(date)
    tk_u.manual_logging_timedelta(load_start_timestamp)
    print('Unloading finished!')
