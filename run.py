import os
import sys
import time
import json
import dbbckp
import fsbckp
import logging
from datetime import datetime

def resource_path(relative_path):
    # Преобразование путей ресурсов в пригодный для использования формат для PyInstaller
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath('.'), relative_path)

def json_adder():
    # Изменяем словарь дат последнего бэкапа приложений, путём слияния словаря файла настроек и словаря, образованного текущей сессией
    json_file = open(resource_path("settings.json"), "r+", encoding='utf8')
    db_data = json.load(json_file)
    db_data["latest_run"] = current_datetime
    json_file.seek(0)
    json.dump(db_data, json_file, ensure_ascii=False, indent=4)
    json_file.truncate()
    json_file.close()

def get_custom_date(date_format):
    # Записываем время запуска программы в переменную, берём формат записи даты в Json-файле
    c_d = datetime.now().strftime(date_format)
    return c_d

def log_conf():
    # Создаём лог-файл
    try:
        os.stat(config['path_to_backups'] + "/logs/")
    except Exception:
        os.makedirs(config['path_to_backups'] + "/logs/")
    open(config['path_to_backups'] + "/logs/" + current_datetime + ".log", 'w').close()

    # Конфигурация логирования
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%d.%m.%Y %H:%M:%S')
    fh = logging.FileHandler(f"{config['path_to_backups']}/logs/{current_datetime}.log", mode='a', encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def delete_expired():
    # Удаляем архивы бэкапов, которым больше чем... (значение в дн. из Json-файла)
    files = [f for f in os.listdir(config['path_to_backups'])]
    expired_files_data = []
    for file in files:
        full_path = config['path_to_backups'] + "/" + file

        is_expired = os.stat(full_path).st_mtime < (time.time() - int(config['backup_age']) * 60 * 60 * 24)
        if is_expired:
            expired_files_data.append(file)
            os.remove(full_path)
    if len(expired_files_data) != 0:
        logger.info(f'Удалено старых файлов бэкапов (спустя: {config["backup_age"]}дн.), итого: {len(expired_files_data)}, {expired_files_data}')

def manual_start(scenario, db_tuple, db_combo, ignr_crt):
    # Сценарий старта бэкапа
    global config
    global logger
    global current_datetime
    config_file = open(resource_path("settings.json"), 'r')
    config = json.load(config_file)  # json-файл конфигурации
    config_file.close()
    logger = logging.getLogger('logger') # Логирование
    current_datetime = get_custom_date(config['dateFormat']) # Время запуска сессии для именования папок (архивов) с документами
    json_adder()  # Обновляем значение времени последнего запуска бэкапа
    log_conf()  # Включаем логирование

    # Сценарии запуска (БД или ФС или ФБ и ФС вместе)
    if scenario == 1:
        dbbckp.dump(cur_datetime=current_datetime, flag='manual', config=config, db_names=db_tuple, db_all=db_combo) #Бэкап БД
    elif scenario == 2:
        fsbckp.mysqlconnect(cur_datetime=current_datetime, flag='manual', config=config, up_to_date=ignr_crt)  # Бэкап ФС
    else:
        dbbckp.dump(cur_datetime=current_datetime, flag='manual', config=config, db_names=db_tuple, db_all=db_combo)  # Бэкап БД
        if not dbbckp.EOFFlag:
            fsbckp.mysqlconnect(cur_datetime=current_datetime, flag='manual', config=config, up_to_date=ignr_crt)  # Бэкап ФС

    if dbbckp.EOFFlag or fsbckp.EOFFlag:
        pass
    else:
        delete_expired()  # Удаляем устаревшие архивы бэкапов

    # Удаляем хендлеры логирования (для предотвращения записи логов в предыдущий(щие) файл(ы)
    handlers = logger.handlers[:]
    for handler in handlers:
        logger.removeHandler(handler)
        handler.close()

    logging.shutdown() # Выключаем логирование

def cron_start(db_tuple, db_combo):
    # Сценарий старта бэкапа
    global config
    global logger
    global current_datetime
    config_file = open(resource_path("settings.json"), 'r')
    config = json.load(config_file)  # json-файл конфигурации
    config_file.close()
    logger = logging.getLogger('logger') # Логирование
    current_datetime = get_custom_date(config['dateFormat']) # Время запуска сессии для именования папок (архивов) с документами
    json_adder() # Обновляем значение времени последнего запуска бэкапа
    log_conf() # Включаем логирование

    # Сценарии запуска (БД или ФС или ФБ и ФС вместе)
    if config['cron_db'] and not config['cron_fs']:
        dbbckp.dump(cur_datetime=current_datetime, flag='manual', config=config, db_names=db_tuple, db_all=db_combo) #Бэкап БД
    elif not config['cron_db'] and config['cron_fs']:
        fsbckp.mysqlconnect(cur_datetime=current_datetime, flag='manual', config=config, up_to_date=config['cron_act_check'])  # Бэкап ФС
    else:
        dbbckp.dump(cur_datetime=current_datetime, flag='manual', config=config, db_names=db_tuple, db_all=db_combo)  # Бэкап БД
        if not dbbckp.EOFFlag:
            fsbckp.mysqlconnect(cur_datetime=current_datetime, flag='manual', config=config, up_to_date=config['cron_act_check'])  # Бэкап ФС

    if dbbckp.EOFFlag or fsbckp.EOFFlag:
        pass
    else:
        delete_expired()  # Удаляем устаревшие архивы бэкапов

    # Удаляем хендлеры логирования (для предотвращения записи логов в предыдущий(щие) файл(ы)
    handlers = logger.handlers[:]
    for handler in handlers:
        logger.removeHandler(handler)
        handler.close()

    logging.shutdown() # Выключаем логирование