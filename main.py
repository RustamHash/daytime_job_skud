#!/user/bin/env python3

# pip3 freeze > requirements.txt

import pandas as pd
from pathlib import Path
from datetime import timedelta as tmd
from datetime import datetime as dt
import glob

_path_default = r"D:\Общая\skud"
_path_file_users = r'D:\Общая\Книга1.xlsx'

daytime_in = tmd(hours=6)
daytime_out = tmd(hours=12)

nighttime_in = tmd(hours=12)
nighttime_out = tmd(days=1, hours=12)


def __generation_dataframe_user(__df_data: pd.DataFrame, _list_users: list, list_period: list) -> dict:
    _dict_date = {}
    for _user in _list_users:
        _dict_user = {}
        _df_temp_user = __df_data[__df_data[1] == _user].copy()
        _df_temp_user[0] = pd.to_datetime(_df_temp_user[0]).copy()
        for _date in list_period:
            _df_temp = _df_temp_user[
                (_df_temp_user['date'] == _date) &
                (_df_temp_user[2] == 'Вход')
                ].copy()
            __in_user = _df_temp[0].min()
            if __in_user.value > 0:
                if (_date + daytime_out) <= __in_user:
                    _df_new = _df_temp_user[
                        (_df_temp_user[0] >= (_date + nighttime_in)) &
                        (_df_temp_user[0] <= (_date + nighttime_out))
                        ].copy()
                    _df_new['date'] = _date
                    _df_new.sort_values(by=[0], inplace=True)
                    _dict_user[_date] = _df_new
                elif (_date + daytime_out) >= __in_user >= (_date + daytime_in):
                    _df_new = _df_temp_user[
                        (_df_temp_user['date'] == _date)
                    ].copy()
                    _df_new.sort_values(by=[0], inplace=True)
                    _dict_user[_date] = _df_new
        _dict_date[_user] = _dict_user
    return _dict_date


def __read_files_period(_period: list) -> pd.DataFrame:
    __file_names: list = []
    for _date in _period:
        _path = Path(_path_default, _date)
        __file_names += [pd.read_csv(filename, sep="\t", encoding='cp1251', header=None, dtype=str) for filename in
                         glob.glob(f'{_path}/*.txt')]
    df_all = pd.concat(__file_names, axis=0)
    df_all.sort_values(by=[0], inplace=True)
    df_all.to_excel(r'all_data.xlsx', index=False)
    return df_all


def __get_fio_is_files() -> list:
    __df = pd.read_excel(_path_file_users)
    list_fio = __df['Сотрудник'].unique().tolist()
    __list_dict_date = []
    for _last_name in list_fio:
        _fio = _last_name.split(' ')
        __list_dict_date.append(
            _fio[0].capitalize() + ' '
            + _fio[1][:1].capitalize() + '.'
            + _fio[2][:1].capitalize() + '.'
        )
    return __list_dict_date


def __filter_users_is_dataframe(__df: pd.DataFrame, __list_users: list) -> pd.DataFrame:
    _df_users = __df[__df[1].isin(__list_users)].copy()
    return _df_users


def __filter_dataframe_to_date(_df_filter: pd.DataFrame, _period: list, _start_date, _end_date) -> pd.DataFrame:
    _df_period = _df_filter[
        (_df_filter['date'] >= _start_date) &
        (_df_filter['date'] <= _end_date)
        ].copy()
    return _df_period


def __create_list_date_period(_start_date, _end_date) -> list:
    _list_date = pd.date_range(start=_start_date, end=_end_date).to_list()
    return _list_date


def __create_end_date_period(_end_date):
    _ = dt.strptime(_end_date, "%Y-%m-%d")
    _ = _ + tmd(days=1)
    _new_end_date = str(_.date())
    return _new_end_date


if __name__ == '__main__':

    start_date = "2023-09-04"
    _end_date = "2023-09-06"
    bool_users = True

    end_date = __create_end_date_period(_end_date)
    _list_period = __create_list_date_period(start_date, _end_date)

    _ = [start_date[:7], end_date[:7]]
    period = list(set(_))
    df = __read_files_period(_period=period)
    df[0] = pd.to_datetime(df[0])
    df['date'] = pd.to_datetime(df[0]).dt.date.copy()
    df['date'] = pd.to_datetime(df['date'])

    if bool_users:
        list_users = __get_fio_is_files()
    else:
        list_users = df[1].unique().tolist()
    __df_users = __filter_users_is_dataframe(__df=df, __list_users=list_users)
    __df_period = __filter_dataframe_to_date(_df_filter=__df_users, _period=period, _start_date=start_date,
                                             _end_date=end_date)
    __dict_users = __generation_dataframe_user(__df_data=__df_period, _list_users=list_users, list_period=_list_period)

    _list_data = []
    _user_dict = {}
    for user, v in __dict_users.items():
        _date_dict = {}
        for date, v1 in v.items():
            _in_user = v1[0][v1[2] == 'Вход'].min()
            _out_user = v1[0][v1[2] == 'Выход'].max()
            _all_time_jobs = _out_user - _in_user
            _d = {'Дата': date, 'ФИО': user, 'Вход': _in_user, 'Выход': _out_user, 'Общее': _all_time_jobs}
            _list_data.append(_d.copy())
            _all = _all_time_jobs + pd.Timestamp(0)
            _date_dict[date.date()] = _all.time()
        _user_dict[user] = _date_dict
    df_validation = pd.DataFrame.from_dict(_list_data, orient='columns')
    df_validation.sort_values(by=['Дата'], inplace=True)
    df_validation.reset_index(inplace=True, drop=True)
    df_validation.index += 1
    df_validation['Дата'] = pd.to_datetime(df_validation['Дата']).dt.date.copy()
    df_validation.to_excel(r'test.xlsx', index=False)

    df_validation['Общее'] = df_validation['Общее'] + pd.Timestamp(0)
    df_validation['Общее'] = df_validation['Общее'].apply(lambda x: x.time() if x.value > 0 else '')
    df_validation.to_excel(r'test.xlsx', index=False)
    df_1 = pd.DataFrame.from_dict(_user_dict, orient='index')
    df_1.to_excel(r'users.xlsx')
