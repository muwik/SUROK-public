import os
import sys
import json
import shutil
import tarfile
import logging
import MySQLdb
from datetime import datetime

logger = logging.getLogger('logger')
EOFFlag = False # Флаг ручного останова бэкапа

def resource_path(relative_path):
    # Преобразование путей ресурсов в пригодный для использования формат для PyInstaller
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath('.'), relative_path)

def write_file(data, filename):
    # Записываем данные в файл
    with open(filename, 'wb') as file:
        if data != None: # Файлы без текста и блоба создаём, но игнорируем отсутствие данных для записи
            file.write(data)
        else: pass

def json_adder(data, json_obj):
    # Изменяем словарь дат последнего бэкапа приложений, путём слияния словаря файла настроек и словаря, образованного текущей сессией
    json_file = open(resource_path("settings.json"), "r+", encoding='utf8')
    db_data = json.load(json_file)
    if json_obj == 'backups_create' or json_obj == 'latest_stencil':
        merger = db_data[json_obj]
        merger.update(data)
        db_data[json_obj] = merger
    elif json_obj == 'latest_fss_ids':
        db_data[json_obj] = data
    json_file.seek(0)
    json.dump(db_data, json_file, ensure_ascii=False, indent=4)
    json_file.truncate()
    json_file.close()

def mysqlconnect(cur_datetime, flag, config, up_to_date=False):
    global EOFFlag
    EOFFlag = False
    # Данные для соединения с БД из Json-файла
    logger.info('-------СТАРТ БЭКАПА ФАЙЛОВОЙ СИСТЕМЫ-------')

    # Данные подключения для вывода в лог
    access_data_log = {'host': config['host'], 'user': config['login'], 'password': '*скрыт*',
                   'db_name': config['fs_db'], 'client_key': config["path_to_ckc"],
                   'server_ca': config["path_to_scc"], 'client_cert': config["path_to_ccc"]}
    logger.info(f'Попытка подключения к базе данных с использованием данных: {access_data_log}')

    # Создаём соединение с БД
    try:
        connection = MySQLdb.connect(
            host=config['host'],
            user=config['login'],
            password=config['password'],
            db=config['fs_db'],
            ssl={'key': config["path_to_ckc"], 'ca': config["path_to_scc"], 'cert': config["path_to_ccc"]}
        )
        cursor = connection.cursor()
        logger.info('Подключение к базе данных успешное')
        logger.info('Начало бэкапа файловой системы')
    except Exception as e:
        logger.critical(f'Ошибка подключения к базе данных! {e}')
        logger.info('-------ФИНИШ БЭКАПА ФАЙЛОВОЙ СИСТЕМЫ С ОШИБКОЙ-------')
        handlers = logger.handlers[:]
        for handler in handlers:
            logger.removeHandler(handler)
            handler.close()
        if flag == 'manual':
            EOFFlag = True
            return
        else:
            sys.exit()

    # Создаём кортеж данных из массива значений (ID проекта, Имя проекта, Дата последнего бэкапа проекта), если проект имеет флаг "L", он бэкапится (знак 1) и является Приложением
    try:
        cursor.execute('''
            SELECT Project_ID, Project_Name, Project_Translit
            FROM projects
            WHERE Project_Status = 'L' AND Project_Backup = '1' AND Project_Project_Type_ID = '2'
            ''')
        fss_for_fsbckp_tuple = cursor.fetchall()  # Массив значений (ID проекта, Имя проекта, Название-транслит)
        ids_for_fsbckp_list = [x[0] for x in fss_for_fsbckp_tuple]  # Список ID проектов из массива выше
    except Exception as e:
        logger.critical(f'Ошибка выполнения запроса к базе данных! {e}')
        logger.info('-------ФИНИШ БЭКАПА ФАЙЛОВОЙ СИСТЕМЫ С ОШИБКОЙ-------')
        handlers = logger.handlers[:]
        for handler in handlers:
            logger.removeHandler(handler)
            handler.close()
        if flag == 'manual':
            EOFFlag = True
            return
        else:
            sys.exit()

    # Создаём аналогичный словарь из кортежа для более простого взаимодействия
    fss_ids_for_fsbckp_dict = dict((x[0], x[1]) for x in fss_for_fsbckp_tuple)  # Словарь. ID - Название
    tfss_ids_for_fsbckp_dict = dict((x[0], x[2].replace(' ', '_')) for x in fss_for_fsbckp_tuple)  # Словарь. ID - Название-транслит

    # Создаём кортеж данных из массива значений (ID бэкапа(ов), ID проекта бэкапа, Дата и время создания бэкапа)
    try:
        if up_to_date:
            cursor.execute(f'''
                SELECT Backup_ID, Backup_Project_ID, Backup_Create
                FROM backups
                WHERE Backup_Device = 'GAE' AND Backup_Project_ID IN ({",".join(("%s",) * len(ids_for_fsbckp_list))})
                ORDER BY Backup_ID
                ASC''', ids_for_fsbckp_list)
        else:
            cursor.execute(f'''
                SELECT Backup_ID, Backup_Project_ID, Backup_Create
                FROM backups
                WHERE Backup_Device = 'GAE' AND Backup_Download IS NULL AND Backup_Project_ID IN ({",".join(("%s",) * len(ids_for_fsbckp_list))})
                ORDER BY Backup_ID
                ASC''', ids_for_fsbckp_list)
        fsbckp_data = cursor.fetchall()  # Кортеж всех бэкапов без флага up_to_date
        fsbckp_data_uptodate_dict = dict((x[1], (x[0], x[2])) for x in fsbckp_data)  # Словарь. ID проекта - (ID последнего бэкапа проекта, Дата и время бэкапа)
        fsbckp_data_uptodate = tuple((fsbckp_data_uptodate_dict[x][0], x, fsbckp_data_uptodate_dict[x][1]) for x in fsbckp_data_uptodate_dict)  # Кортеж со значениями из словаря выше. Создан, т.к. применение словаря для бэкапа ФС без флага up_to_date - невозможно. Ключи должны быть уникальными.
        if up_to_date:
            # В зависимости от наличия/отсутствия флага up_to_date, даём на парсинг определённый кортеж
            fsbckp_data_to_parse = fsbckp_data_uptodate
        else:
            fsbckp_data_to_parse = fsbckp_data
        fsbckp_id_date_dict = dict((x[0], x[2]) for x in fsbckp_data_to_parse)  # Словарь актуальных данных для парсинга. ID бэкапа - Дата и время бэкапа
    except Exception as e:
        logger.critical(f'Ошибка выполнения запроса к базе данных! {e}')
        logger.info('-------ФИНИШ БЭКАПА ФАЙЛОВОЙ СИСТЕМЫ С ОШИБКОЙ-------')
        handlers = logger.handlers[:]
        for handler in handlers:
            logger.removeHandler(handler)
            handler.close()
        if flag == 'manual':
            EOFFlag = True
            return
        else:
            sys.exit()

    actual_value = {}  # Если есть новая версия бэкапа проекта, в словарь добавляется ID проекта (ключ) и дата его бэкапа (значение)
    all_backups = [x[0] for x in fsbckp_data_to_parse] # Список ID всех бэкапов для сессии скрипта
    created_projects = [] # Сохранённые проекты (список ID проектов)
    created_backups = []  # Сохранённые бэкапы (список ID бэкапов)
    empty_backups = []  # Список бэкапов, которые не содержат файлы
    backuped_projects = set() # Сет забэкапленных преоктов
    for backup_instance in fsbckp_data_to_parse:
        # Цикл прохода по бэкапам. Получаем список списков с данными файлов.
        # Создаём кортеж данных из массива значений (ID бэкапа, Путь к файлу в структуре ФС, Имя файла, Содержимое файла)
        if EOFFlag:
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            break

        try:
            cursor.execute('''
                SELECT Backup_File_Backup_ID, Backup_File_Path, Backup_File_Name, Backup_File_Content
                FROM backup_files
                WHERE Backup_File_Backup_ID = %s
                ''', [backup_instance[0]])
            fs_fsbckp_data = cursor.fetchall()

            # Запись в переменную даты бэкапа данного проекта
            actual_value[str(backup_instance[1])] = fsbckp_id_date_dict[backup_instance[0]].strftime(config['dateFormat'])
        except Exception as e:
            logger.critical(f'Ошибка выполнения запроса к базе данных! {e}')
            logger.info('-------ФИНИШ БЭКАПА ФАЙЛОВОЙ СИСТЕМЫ С ОШИБКОЙ-------')
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            if flag == 'manual':
                EOFFlag = True
                break
            else:
                sys.exit()

        for file_data in fs_fsbckp_data:
            # Цикл прохода по файлам бэкапа
            # Воссоздаём файловую систему + записываем файлы
            try:
                os.stat(config["path_to_backups"] + "/" + cur_datetime + "(" + str(backup_instance[0]) + ")" + "_app_" + tfss_ids_for_fsbckp_dict[backup_instance[1]] + "/" + file_data[1])
            except FileNotFoundError:
                os.makedirs(config["path_to_backups"] + "/" + cur_datetime + "(" + str(backup_instance[0]) + ")" + "_app_" + tfss_ids_for_fsbckp_dict[backup_instance[1]] + "/" + file_data[1])
            write_file(file_data[3], config["path_to_backups"] + "/" + cur_datetime + "(" + str(backup_instance[0]) + ")" + "_app_" + tfss_ids_for_fsbckp_dict[backup_instance[1]] + "/" + file_data[1] + file_data[2])

    logger.info('Файловая система успешно воссоздана')
    logger.info('Начало архивации бэкапов файловой системы')
    for backup_instance in fsbckp_data_to_parse:
        # Архивируем поочередно папки с бэкапом + удаляем папку после архивации
        if EOFFlag:
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            break

        try:
            if os.path.exists(config["path_to_backups"] + "/" + cur_datetime + "(" + str(backup_instance[0]) + ")" + "_app_" + tfss_ids_for_fsbckp_dict[backup_instance[1]]):
                try:
                    with tarfile.open(config["path_to_backups"] + "/" + cur_datetime + "(" + str(backup_instance[0]) + ")" + "_app_" + tfss_ids_for_fsbckp_dict[backup_instance[1]] + ".tar.gz", "w:gz") as tar:
                        tar.add(config["path_to_backups"] + "/" + cur_datetime + "(" + str(backup_instance[0]) + ")" + "_app_" + tfss_ids_for_fsbckp_dict[backup_instance[1]], arcname=os.path.basename(config["path_to_backups"] + "/" + cur_datetime + "(" + str(backup_instance[0]) + ")" + "_app_" + tfss_ids_for_fsbckp_dict[backup_instance[1]]))
                    shutil.rmtree(config["path_to_backups"] + "/" + cur_datetime + "(" + str(backup_instance[0]) + ")" + "_app_" + tfss_ids_for_fsbckp_dict[backup_instance[1]])
                except Exception as e:
                    logger.critical(f'Ошибка архивации файла бэкапа! {e}')
                    logger.info('-------ФИНИШ БЭКАПА ФАЙЛОВОЙ СИСТЕМЫ С ОШИБКОЙ-------')
                    handlers = logger.handlers[:]
                    for handler in handlers:
                        logger.removeHandler(handler)
                        handler.close()
                    if flag == 'manual':
                        EOFFlag = True
                        break
                    else:
                        sys.exit()
                created_backups.append(backup_instance[0])
                created_projects.append(str(backup_instance[1]))
                try:
                    secval = cur_datetime + "(" + str(backup_instance[0]) + ")" + "_app_" + tfss_ids_for_fsbckp_dict[backup_instance[1]] + ".tar.gz"
                    q_d = datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M') - datetime.strptime(datetime.utcnow().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M')  # Сдвиг по UTC
                    r_d = datetime.strptime(cur_datetime, '%Y_%m_%d_%H_%M') - q_d
                    formatted_cur_datetime_tuple = str(r_d.strftime('%Y_%m_%d_%H_%M')).split('_')
                    formatted_cur_datetime = f'{formatted_cur_datetime_tuple[0]}-{formatted_cur_datetime_tuple[1]}-{formatted_cur_datetime_tuple[2]} {formatted_cur_datetime_tuple[3]}:{formatted_cur_datetime_tuple[4]}'
                    # Записываем данные в таблицу backups, от имени второго бэкапера
                    cursor.execute('''INSERT INTO backups (`Backup_Device`, `Backup_Project_ID`, `Backup_File_Name`, `Backup_Create`, `Backup_Note`)
                                        VALUES ('SUROK', %s, %s, %s, '')''', (backup_instance[1], secval, formatted_cur_datetime))
                    connection.commit()
                    cursor.execute('''SELECT
                        MAX(Backup_ID)
                        FROM backups;''')
                    latest_backup_id = cursor.fetchall()[0][0]
                    # Дублируем данные в таблице backup_files
                    cursor.execute('''INSERT INTO backup_files (Backup_File_Backup_ID, Backup_File_Path, Backup_File_Name, Backup_File_Size, Backup_File_Content)  
                        SELECT %s, Backup_File_Path, Backup_File_Name, Backup_File_Size, Backup_File_Content
                          FROM backup_files
                         WHERE Backup_File_Backup_ID = %s''', [latest_backup_id, backup_instance[0]])
                except Exception as e:
                    logger.critical(f'Ошибка выполнения запроса к базе данных! {e}')
                    logger.info('-------ФИНИШ БЭКАПА ФАЙЛОВОЙ СИСТЕМЫ С ОШИБКОЙ-------')
                    handlers = logger.handlers[:]
                    for handler in handlers:
                        logger.removeHandler(handler)
                        handler.close()
                    if flag == 'manual':
                        EOFFlag = True
                        break
                    else:
                        sys.exit()
            else:
                empty_backups.append(backup_instance[0])
        except KeyError:
            empty_backups.append(backup_instance[0])
        backuped_projects.add(backup_instance[1])

    if not EOFFlag:
        # Проставляем флаг "1" о скачивании бэкапов и удаляем все данные файлов в backup_files
        cursor.execute(f'''
            UPDATE backups    
            SET Backup_Download = 1
            WHERE Backup_Device = 'GAE' AND Backup_Project_ID IN ({",".join(('%s',) * len(ids_for_fsbckp_list))})''', ids_for_fsbckp_list)
        connection.commit()
        cursor.execute(f'''
            UPDATE backup_files
            SET Backup_File_Content = NULL
            WHERE Backup_File_Content is not null;''')
        connection.commit()
        # Записываем в json-файл актуальные значения по проектам/бэкапам
        latest_stencil = {(str(x), fss_ids_for_fsbckp_dict[x]) for x in fss_ids_for_fsbckp_dict}
        json_adder(actual_value, 'backups_create')
        json_adder(list(set(created_projects)), 'latest_fss_ids')
        json_adder(latest_stencil, 'latest_stencil')
        missing_projects = [fss_ids_for_fsbckp_dict[project] for project in ids_for_fsbckp_list if project not in backuped_projects]

        if len(created_backups) != 0:
            logger.info('Файловая система успешно архивирована')
        logger.info(f'Обработанных бэкапов ФС, итого: {len(all_backups)}, {all_backups}')
        logger.info(f'Сохранённых бэкапов ФС, итого: {len(created_backups)}, {created_backups}')
        if len(empty_backups) > 0:
            logger.warning(f'Пустых бэкапов ФС, итого: {len(empty_backups)}, {empty_backups}')
        if len(missing_projects) > 0:
            logger.warning(f'Отсутствующих проектов ФС, итого: {len(missing_projects)}, {missing_projects}')
        logger.info('-------ФИНИШ БЭКАПА ФАЙЛОВОЙ СИСТЕМЫ-------')
    # Закрываем соединение
    connection.close()