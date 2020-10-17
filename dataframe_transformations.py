"""
This module contains functions for work with tabular data (for pins, unformed, orders
and geographic Pandas dataframes).
and get_geozones function.
"""
import logging

import numpy as np
import pandas as pd

import CONSTANTS
import TK_utils as tk_u
import renaming_dicts


def modify_and_save_pins(df, car_classes_df, cities_df, geo_df, date):
    """
    Transforms and saves to file pins dataframe
    :param df: Pandas DataFrame with pins
    :param car_classes_df: Pandas DataFrame with car classes codes and names
    :param cities_df: Pandas DataFrame with cities id's and names
    :param geo_df: Pandas DataFrame with geo zones names, their boundary points and city names
    :param date: date to load in "YYYY-MM-DD" format
    :return: None, but saving file to local network server "\\bigshare\Выгрузки ТФ\Выгрузки My_TK\'year'\'month'"
    """
    # Merge car classes
    df = df.merge(car_classes_df, left_on='type_auto', right_on='id', how='left')  # retrieve car classes names
    df.drop(columns=['type_auto', 'id'], inplace=True)  # cleaning after merge
    df.rename({'name': 'type_auto'}, axis='columns', inplace=True)  # cleaning after merge
    # Merge cities
    df = df.merge(cities_df, left_on='city', right_on='id', how='left')
    df = df[df.type.str.startswith('taxi', na=False)]
    df.drop(columns=['id', 'type', 'city', 'to_local_time_corr'], inplace=True)
    df.rename(columns={'name': 'city'}, inplace=True)
    # Incoming source mapping
    df['come_from'] = df.come_from.astype(str).map(renaming_dicts.incoming_type)
    # Separate 'date' column to date and time
    df['dat'] = pd.to_datetime(df.dat)
    new_dates, new_times = zip(*[(d.date(), d.time()) for d in df['dat']])
    df = df.assign(Дата=new_dates, Время=new_times)
    df.drop(columns='dat', inplace=True)
    # Map geo zones
    df.rename(columns={'x': 'x_in', 'y': 'y_in'}, inplace=True)
    get_zone(df=df, geozone_df=geo_df, mode='in')
    # Generate key field
    df['Номер_пина'] = df.Дата.astype(str).str.replace('-', '', regex=True).apply(lambda x: x[-4:]) + \
                       df.Время.astype(str).str.replace(':', '', regex=True) + \
                       df.id_client.astype(str)
    # Final renaming, dropping and saving
    df.rename(renaming_dicts.pin, axis='columns', inplace=True)
    df['Статус'] = 'Пин'
    df = df.replace(r'^\s*$', np.NaN, regex=True)  # replace all empty strings with NaNs
    # df.to_csv(f"data/{date}_пины.csv", sep=';', index=False)
    saving_path = tk_u.set_bigshare_dir(date)
    df.to_csv(
        f"{saving_path}/{date}_пины.csv",
        sep=';', index=False)
    # load_to_GDrive(f"data/{date}_пины.csv")


def modify_and_save_unformed(df, car_classes_df, cities_df, geo_df, date):
    """
    Transforms and saves to file unformed orders dataframe
    :param df: Pandas DataFrame with unformed orders
    :param car_classes_df: Pandas DataFrame with car classes codes and names
    :param cities_df: Pandas DataFrame with cities id's and names
    :param geo_df: Pandas DataFrame with geo zones names, their boundary points and city names
    :param date: date to load in "YYYY-MM-DD" format
    :return: None, but saving file to local network server "\\bigshare\Выгрузки ТФ\Выгрузки My_TK\'year'\'month'"
    """
    df['points'] = df['points'].apply(len)  # transitional points list to len of that list
    df['type_auto'] = df.type_auto.astype(int)
    # df['is_taxo'] = df.is_taxo.replace('', np.NaN).astype(bool)  # bool casts missing values to False
    df['is_taxo'] = pd.array(df.is_taxo.replace('', np.NaN), dtype=pd.Int8Dtype())  # so I use Int8
    # df['base_price'] = pd.array(df.base_price, dtype=pd.Int32Dtype())  # To keep INT with NaNs
    # df['base_price2'] = pd.array(df.base_price2, dtype=pd.Int32Dtype())  # To keep INT with NaNs
    df['base_price'] = df['base_price'].fillna(0).astype(int)
    df['base_price2'] = df['base_price2'].fillna(0).astype(int)
    df['proc_a_in'] = df.proc_a_in.astype(int) / 100
    df = df.merge(car_classes_df, left_on='type_auto', right_on='id', how='left')  # retrieve car classes names
    df.drop(columns=['type_auto', 'id'], inplace=True)  # cleaning after merge
    df.rename({'name': 'type_auto'}, axis='columns', inplace=True)  # cleaning after merge
    # Extract min car serving time from autos_time
    for idx, row in df.iterrows():
        min_car_time = 100500
        if row['autos_time'] == '':
            df.loc[idx, 'autos_time'] = np.NaN
            continue
        for time in (dct['time'] for dct in row['autos_time']):
            min_car_time = min(time, min_car_time)
        if min_car_time == 100500:
            min_car_time = np.NaN
        df.loc[idx, 'autos_time'] = min_car_time
    # df['autos_time'] = df.autos_time.astype(int)
    df['autos_time'] = pd.array(df.autos_time, dtype=pd.Int16Dtype())
    # Merge with cities names df and drop non-taxi entries
    df = df.merge(cities_df, left_on='city', right_on='id', how='left', suffixes=('', '_source'))
    df = df[df.type_source.str.startswith('taxi', na=False)]
    df.drop(columns=['id', 'type_source', 'city', 'to_local_time_corr'], inplace=True)
    df.rename(columns={'name': 'city'}, inplace=True)
    # Separate 'date' column to date and time
    df['date'] = pd.to_datetime(df.date)
    new_dates, new_times = zip(*[(d.date(), d.time()) for d in df['date']])
    df = df.assign(Дата=new_dates, Время=new_times)
    df.drop(columns='date', inplace=True)
    # Drop duplicates!
    df.drop_duplicates(subset=['Дата', 'Время', 'phone'], ignore_index=True, inplace=True)
    # Incoming source mapping
    df['type'] = df.type.astype(str).map(renaming_dicts.incoming_type)
    # Get rid of possible bug entries
    df = df[df.x_in != 0.]
    # Map geo zones
    get_zone(df=df, geozone_df=geo_df, mode='in')
    get_zone(df=df, geozone_df=geo_df, mode='out')
    # Generate key field: MMDDhhmmss&id (or &phone[-7:] if id == 0)
    # df['Номер_неоформленного'] = df.Дата.astype(str).str.replace('-', '', regex=True).apply(lambda x: x[-4:]) + \
    #                              df.Время.astype(str).str.replace(':', '', regex=True) + \
    #                              df.id_client.astype(str)
    df['Номер_неоформленного'] = np.where(df.id_client == 0,
                                          df.Дата.astype(str).str.replace('-', '', regex=True).apply(lambda x: x[-4:]) + \
                                          df.Время.astype(str).str.replace(':', '', regex=True) + \
                                          df.phone.astype(str).apply(lambda x: x[-7:]),
                                          df.Дата.astype(str).str.replace('-', '', regex=True).apply(lambda x: x[-4:]) + \
                                          df.Время.astype(str).str.replace(':', '', regex=True) + \
                                          df.id_client.astype(str))
    # Final renaming, dropping and saving
    df.rename(renaming_dicts.unf, axis='columns', inplace=True)
    df.drop(columns=['option_1', 'option_2', 'option_3',
                     'c_auto_all', 'proc_a_in_all', 'id_user'], inplace=True)
    df['Статус'] = 'Неоформленный'
    df = df.replace(r'^\s*$', np.NaN, regex=True)  # replace all empty strings with NaNs
    # df.to_csv(f"data/{date}_неоф.csv", sep=';', index=False)
    saving_path = tk_u.set_bigshare_dir(date)
    df.to_csv(
        f"{saving_path}/{date}_неоф.csv",
        sep=';', index=False)
    # load_to_GDrive(f"data/{date}_неоф.csv")


def modify_and_save_orders(df, car_classes_df, cities_df, options_df, geo_df, date):
    """
    Transforms and saves to file orders dataframe
    :param df: Pandas DataFrame with orders
    :param car_classes_df: Pandas DataFrame with car classes codes and names
    :param cities_df: Pandas DataFrame with cities id's and names
    :param options_df: Pandas DataFrame with options id's and names
    :param geo_df: Pandas DataFrame with geo zones names, their boundary points and city names
    :param date: date to load in "YYYY-MM-DD" format
    :return: None, but saving file to local network server "\\bigshare\Выгрузки ТФ\Выгрузки My_TK\'year'\'month'"
    """
    df.drop(columns=['id_user_out', 'c_zalog', 'text_breach', 'text_re_breach', 'note_control',
                     'cancel_note_adm', 'cancel_note_okk', 'name_type_auto'
                     ], inplace=True)
    df.drop_duplicates(subset='id', keep='last', inplace=True)  # id == Номер
    df['note'] = df['note'].str.replace(r'\r\n|\r|\n|\t', ' ')  # Delete damn escape-characters
    df['company_answer'] = df['company_answer'].str.replace(r'\r\n|\r|\n|\t', ' ')
    # Trim names
    df['client_name'] = df['client_name'].str.strip()
    df['client_name'] = df['client_name'].replace(r'\r\n|\r|\n|\t', ' ')
    df['contact_client_name'] = df['contact_client_name'].str.strip()
    df['contact_client_name'] = df['contact_client_name'].replace(r'\r\n|\r|\n|\t', ' ')
    # Change date/time types
    df['dat'] = pd.to_datetime(df['dat'])
    df['dat'] = df.dat.dt.date
    df['dat_add'] = pd.to_datetime(df['dat_add'])
    df['dat_out'] = pd.to_datetime(df['dat_out'])
    df['driver_dat_a_in'] = pd.to_datetime(df['driver_dat_a_in'])
    df['ed_22'] = pd.to_datetime(df['ed_22'])
    df['dat_close'] = pd.to_datetime(df['dat_close'])
    df['dat_cancel'] = pd.to_datetime(df['dat_cancel'])
    # Transform the time format to the adequate one
    df['time_'] = df.time_.map(tk_u.time_transform)
    # Make 'Date and time of arrival' field
    df['Дата и время подачи'] = df.dat.astype(str) + ' ' + df.time_
    df['Дата и время подачи'] = pd.to_datetime(df['Дата и время подачи'])
    df['Дата и время подачи'] = df['Дата и время подачи'].dt.strftime('%d.%m.%Y %H:%M')
    # Merge with cities names df, drop non-taxi entries, cast moscow time to local
    df['city_'] = df.city_.replace('', 0).astype(int)  # empty strings has id equal 0
    df = df.merge(cities_df, left_on='city_', right_on='id', how='left', suffixes=('', '_source'))
    df = df[df.type.str.startswith('taxi', na=False)]
    # TODO: Correct all regional cities times with respect to timezones!!!
    df['dat_add'] = df.dat_add + df.to_local_time_corr
    df['dat_out'] = df.dat_out + df.to_local_time_corr
    df['driver_dat_a_in'] = df.driver_dat_a_in + df.to_local_time_corr
    df['ed_22'] = df.ed_22 + df.to_local_time_corr
    df['dat_close'] = df.dat_close + df.to_local_time_corr
    df['dat_cancel'] = df.dat_cancel + df.to_local_time_corr
    df.drop(columns=['id_source', 'type', 'city_', 'to_local_time_corr'], inplace=True)
    df.rename(columns={'name': 'city'}, inplace=True)
    # Coords type cast
    df.rename(columns={'x_out_': 'x_out', 'y_out_': 'y_out'}, inplace=True)
    df.drop(df[df.x_in == ''].index, axis=0, inplace=True)
    df.drop(df[df.x_out == ''].index, axis=0, inplace=True)
    df.drop(df[df.y_in == ''].index, axis=0, inplace=True)
    df.drop(df[df.y_out == ''].index, axis=0, inplace=True)
    df['x_in'] = df.x_in.astype(float)
    df['x_out'] = df.x_out.astype(float)
    df['y_in'] = df.y_in.astype(float)
    df['y_out'] = df.y_out.astype(float)
    # Pickup zone's percent
    df.hexo_proc_a_in.replace('', np.NaN, inplace=True)
    df['hexo_proc_a_in'] = df.hexo_proc_a_in.astype(float)
    # Other type transformations and empty field replacements
    df['park_'] = df.park_.replace('', 0).astype(int) + 1
    df['dr_minimum'] = df.dr_minimum.replace('', '0').astype(int)  # Driver's minimal payment
    df['p_auto'] = df.p_auto.replace('', '0').astype(int)
    df['pp_sum'] = df.pp_sum.replace('', '0').astype(int)
    df['pp_min'] = df.pp_min.replace('', '0').astype(int)
    df['pp_min_4'] = df.pp_min_4.replace('', '0').astype(int)
    df['c_auto'] = df.c_auto.replace('', '0').astype(int)
    df['c_auto_b'] = df.c_auto_b.replace('', '0').astype(int)
    df['pp_sum'] = df.pp_sum.replace('', '0').astype(int)
    df['driver_perc'] = df.driver_perc.replace(r'^\s*$', np.NaN, regex=True).astype(float)
    df['c_auto_2'] = df.c_auto_2.replace('', '0').astype(int)
    df['oper_pay'] = df.oper_pay.replace('', '0').astype(int)
    df['ap_dist'] = df.ap_dist.replace('', '0').astype(int)
    df['client_minimalka'] = df.client_minimalka.replace('', '0').astype(int)
    df['slice_pr_by_hexo'] = df.slice_pr_by_hexo.replace('', '0').astype(int)
    df['base_price'] = df.base_price.replace('', '0').astype(int)
    df['base_price2'] = df.base_price2.replace('', '0').astype(int)
    df['time_ed3'] = df.time_ed3.replace('', '0').astype(int)
    df['time_ed0'] = df.time_ed0.replace('', '0').astype(int)
    df['time2'] = df.time2.replace('', '0').astype(int)
    df['dist1'] = df.dist1.replace('', '0').astype(int)
    df['dist2'] = df.dist2.replace('', '0').astype(int)
    df['p_driver_s'] = df.p_driver_s.str.replace(r'^\s*$', '0', regex=True)
    df['p_driver_s'] = df['p_driver_s'].astype(int)
    df['warn'] = df.warn.replace('', '0').astype(int)
    df['dr_opt'] = df.dr_opt.replace('', '0').astype(int)
    # replace all values in columns with the value of 10th/1st bit
    df['warn'] = df.warn.apply(tk_u.is_bit_setted, args=(10,))
    df['dr_opt'] = df.dr_opt.apply(tk_u.is_bit_setted, args=(1,))

    # Driver's part
    # Tips (5%, 10% and 15%) included. We can't count them separately without API improvement
    # 1. Private drivers and our drivers with car is in 'раскат' [1st bit of DR_OPT is set]
    # 1.1 Add (order's and paid waiting's cost) to p_driver_s IF:
    # payment type is 'Наличный' OR 'Залог' OR ('Картой вод' AND driver has personal terminal) [10th bit of WARN is set]
    df['p_driver_s'] = np.where(
        ((df.our_driver != '0') | ((df.our_driver == '0') & df.dr_opt) ) &
        ((df.type_money == '0') |
        (df.type_money == '2') |
        (df.type_money == '12') |
        ((df.type_money == '5') & df.warn)),
        df.p_driver_s + df.c_auto + df.pp_sum,
        df.p_driver_s)
    # 1.2 Add (order's and paid services's cost) to p_driver_s IF:
    # payment type 2 is 'Наличный' OR 'Залог' OR ('Картой вод' AND driver has personal terminal) [10th bit of WARN is set]
    df['p_driver_s'] = np.where(
        ((df.our_driver != '0') | ((df.our_driver == '0') & df.dr_opt) ) &
        ((df.type_money_b == '0') |
        (df.type_money_b == '2') |
        (df.type_money_b == '12') |
        ((df.type_money_b == '5') & df.warn)),
        df.p_driver_s + df.c_auto_b,
        df.p_driver_s)
    # 2 Cashless payment
    # Subtract from p_driver_s paid services's cost IF payment type is 'Безнал'
    df['p_driver_s'] = np.where(
        ((df.type_money != '0') &
         (df.type_money != '2') &
         (df.type_money != '12') &
         np.logical_not((df.type_money == '5') & df.warn)),
        df.p_driver_s - df.p_auto,
        df.p_driver_s)
    # make part of the driver = 0 for cancelled orders
    df['p_driver_s'] = np.where(df.status == '3', 0, df.p_driver_s)

    # Required auto type merge
    df['type_auto'] = df.type_auto.astype(int)
    df = df.merge(car_classes_df, left_on='type_auto', right_on='id', how='left', suffixes=('', '_classes'))
    df.drop(columns=['type_auto', 'id_classes'], inplace=True)  # cleaning after merge
    df.rename({'name': 'type_auto'}, axis='columns', inplace=True)  # cleaning after merge
    # Executed auto type merge
    df['c_type_auto'].replace('', '-1', inplace=True)
    df['c_type_auto'] = df.c_type_auto.astype(int)
    df = df.merge(car_classes_df, left_on='c_type_auto', right_on='id', how='left', suffixes=('', '_classes'))
    df.drop(columns=['c_type_auto', 'id_classes'], inplace=True)
    df.rename(columns={'name': 'c_type_auto'}, inplace=True)
    # Order options merge
    df['option_1'] = df.option_1.str.replace(r'^\s*$', '0', regex=True).apply(int, base=2)
    df['option_2'] = df.option_2.str.replace(r'^\s*$', '0', regex=True).apply(int, base=2)
    df['option_3'] = df.option_3.str.replace(r'^\s*$', '0', regex=True).apply(int, base=2)
    df['opt_4'] = df['opt_4'].str.replace(r'^\s*$', '0', regex=True).astype(int)
    extract_ride_options(df=df, opt_df=options_df)
    # Create 'ПРЦ предлагалось', 'ПРЦ использовано' and 'Расчет по базе 2?' columns
    df['stat_opt'] = df.stat_opt.replace('', '0').astype(int)
    unfold_stat_opt(df=df)
    # Drivers from 'Обменник'
    df['family_driver'] = np.where(df['driver'] == '-1',
                                   'Водитель из обменника',
                                   df['family_driver'])
    #df.driver.replace('-1', np.NaN, inplace=True)
    # Addresses transform
    df['Адрес подачи'] = df['a_in'] + ' ' + df['a_in_house']
    df['Адреса назначения'] = df['a_out'] + ' ' + df['a_out_house']
    # Status mapping
    df['status'] = df.status.map(renaming_dicts.ord_status)
    # Our / owner-driver mapping
    df['our_driver'] = df.our_driver.map(renaming_dicts.our_or_owner_driver)  # .fillna(df['our_driver'])
    # Payment type mapping
    df['type_money'] = df.type_money.map(renaming_dicts.payment_type)
    df['type_money_b'] = df.type_money_b.map(renaming_dicts.payment_type)
    # Incoming source mapping
    df['come_from'] = df.come_from.map(renaming_dicts.incoming_type)
    # Get rid of possible bug entries
    df = df[df.x_in != 0.]
    df = df[df.x_out != 0.]
    # Map geo zones
    get_zone(df=df, geozone_df=geo_df, mode='in')
    get_zone(df=df, geozone_df=geo_df, mode='out')
    # Final renaming, dropping and saving
    df.rename(renaming_dicts.ord, axis='columns', inplace=True)
    df.drop(columns=['a_in', 'a_in_house', 'a_out', 'a_out_house', 'stat_opt',
                     'option_1', 'option_2', 'option_3', 'opt_4', 'warn', 'dr_opt'], inplace=True)
    df.replace(r'^\s*$', np.NaN, regex=True, inplace=True)  # replace all empty strings with NaNs
    # df.to_csv(f"data/{date}_заказы.csv", sep=';', index=False)
    saving_path = tk_u.set_bigshare_dir(date)
    df.to_csv(
        f"{saving_path}/{date}_заказы.csv",
        sep=';', index=False)
    # load_to_GDrive(f"data/{date}_заказы.csv")


def get_geozones(df, cities_df):
    """
    Transforms geo zone dataframe
    :param df: Pandas DataFrame with geo zones, their compressed boundaries and city ids
    :param cities_df: Pandas DataFrame with cities id's and names
    :return: Pandas DataFrame with geo zones names, their boundary points and city names
    """
    df['city'] = df['city'].astype(int)
    df['compressed_boundary'] = df.compressed_boundary.apply(tk_u.decode_way)  # decompress border coordinates
    df = df.merge(cities_df, left_on='city', right_on='id', how='left')
    df = df[df.type.str.startswith('taxi', na=False)]
    df.drop(columns=['id', 'type', 'city'], inplace=True)
    df.rename(columns={'name': 'city', 'name_': 'geozone'}, inplace=True)
    df.drop_duplicates(subset=['geozone', 'city'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    # df['barycenter'] = df.compressed_boundary.apply(calc_barycenter)
    # Excel bug: in excel some city values appear to be empty, but dataframe is whole:
    df.to_csv(r"match_tables/geozones.csv", sep=';', index=False)
    return df


def get_zone(df, geozone_df, mode='in'):
    """
    Calculates zone for given X and Y coordinates depends on mode and creates column 'Название зоны...'.
    :df: df for creating new geoname column (pin_unf_df/orders_df)
    :geozone_df: df with decompressed boundaries, city and geozone names
    :mode: 'in' - generates 'Название зоны подачи', 'out' - generates 'Название зоны назначения'. Default 'in'
    :return: None
    """
    logger = logging.getLogger(__name__)
    geo_cities_array = geozone_df.city.unique()
    for idx, row in df.iterrows():  # iterate through dataframe
        # TODO: bug. Marks out zone by the city of input zone
        if row.city not in geo_cities_array:  # if there are no such city in geozones table
            if row.city in CONSTANTS.spb_list:  # if city one of suburbs in big city then city slice = that city
                city_slice = geozone_df[geozone_df['city'] == 'Санкт-Петербург']
            elif row.city in CONSTANTS.msk_list:
                city_slice = geozone_df[geozone_df['city'] == 'Москва']
            elif mode == 'in':
                df.loc[idx, 'Название зоны подачи'] = row.loc['city'] + ' (неразмеч. город)'
                continue
            elif mode == 'out':
                df.loc[idx, 'Название зоны назначения'] = row.loc['city'] + ' (неразмеч. город)'
                continue
            else:
                logger.error(f"City {row.city} not in geo_df and no mode setted")
                continue
        else:
            city_slice = geozone_df[geozone_df['city'] == row.city]
        if city_slice.shape[0] == 1:  # case if only one zone name tagged to current city
            if mode == 'in':
                df.loc[idx, 'Название зоны подачи'] = city_slice.reset_index().loc[0, 'geozone']
                continue
            if mode == 'out':
                df.loc[idx, 'Название зоны назначения'] = city_slice.reset_index().loc[0, 'geozone']
                continue
        if mode == 'in':
            point_x = row.loc['x_in']
            point_y = row.loc['y_in']
        elif mode == 'out':
            point_x = row.loc['x_out']
            point_y = row.loc['y_out']
        else:
            raise ValueError('Unknown mode!')
        # calculations:
        for tmp_idx, tmp_row in city_slice.iterrows():
            if tk_u.is_in_polygon(X=point_x, Y=point_y, polygon=city_slice.loc[tmp_idx, 'compressed_boundary']):
                if mode == 'in':
                    df.loc[idx, 'Название зоны подачи'] = city_slice.loc[tmp_idx, 'geozone']
                if mode == 'out':
                    df.loc[idx, 'Название зоны назначения'] = city_slice.loc[tmp_idx, 'geozone']
                break
        else:  # runs if for loop ends w/o break => without finding proper polygon
            if mode == 'in':
                df.loc[idx, 'Название зоны подачи'] = row.loc['city'] + ' (неразмеч. зона)'
            if mode == 'out':
                df.loc[idx, 'Название зоны назначения'] = row.loc['city'] + ' (неразмеч. зона)'
    # logger.info(f"df with size {df.size} mapped with zones in '{mode}' mode")


def unfold_stat_opt(df):
    """
    Adds 3 new columns to df, based on bits, setted in stat_opt field
    :param df: Pandas DataFrame with orders
    :return: None
    """

    for idx, row in df.iterrows():  # iterate through dataframe
        if tk_u.is_bit_setted(row.stat_opt, n_of_bit=0):
            df.loc[idx, 'ПРЦ предлагалось'] = 'Да'
        else:
            df.loc[idx, 'ПРЦ предлагалось'] = 'Нет'
        if tk_u.is_bit_setted(row.stat_opt, n_of_bit=1):
            df.loc[idx, 'ПРЦ использовано'] = 'Да'
        else:
            df.loc[idx, 'ПРЦ использовано'] = 'Нет'
        if tk_u.is_bit_setted(row.stat_opt, n_of_bit=2):
            df.loc[idx, 'Расчет по базе 2?'] = 'Да'


def extract_ride_options(df, opt_df):
    """
    Creates new column 'Опци_заказа'. Fills it with respectively set bits
    in 'options_1', '...2', '...3' and 'opt_4'
    :param df: orders dataframe
    :param opt_df: options dataframe
    :return: None
    """
    df['Опции_заказа'] = ''
    for idx, row in df.iterrows():
        for bit in range(10):
            if tk_u.is_bit_setted(row['option_1'], n_of_bit=bit):
                df.loc[idx, 'Опции_заказа'] += opt_df.loc[opt_df['id_option'] == (bit + 1),'name'][bit] + ', '
        for bit in range(10):
            if tk_u.is_bit_setted(row['option_2'], n_of_bit=bit):
                df.loc[idx, 'Опции_заказа'] += opt_df.loc[opt_df['id_option'] == (bit + 11),'name'][bit + 10] + ', '
        for bit in range(10):
            if tk_u.is_bit_setted(row['option_3'], n_of_bit=bit):
                df.loc[idx, 'Опции_заказа'] += opt_df.loc[opt_df['id_option'] == (bit + 21),'name'][bit + 20] + ', '
        for bit in range(7):
            if tk_u.is_bit_setted(row['opt_4'], n_of_bit=bit):
                df.loc[idx, 'Опции_заказа'] += opt_df.loc[opt_df['id_option'] == (bit + 31),'name'][bit + 30] + ', '
    df['Опции_заказа'] = np.where(df.Опции_заказа == '', np.NaN, df.Опции_заказа.str.replace(', $', '', regex=True))