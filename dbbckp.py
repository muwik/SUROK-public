import os
import sys
import json
import logging
import MySQLdb
import subprocess
from datetime import datetime

logger = logging.getLogger('logger')
EOFFlag = False # Флаг ручного останова бэкапа
dump_call = None # Команда подпроцесса дампа БД
arch_call = None # Команда подпроцесса архивирования БД

def resource_path(relative_path):
    # Преобразование путей ресурсов в пригодный для использования формат для PyInstaller
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath('.'), relative_path)

def json_adder(data, json_obj):
    # Изменяем словарь дат последнего бэкапа приложений, путём слияния словаря файла настроек и словаря, образованного текущей сессией
    json_file = open(resource_path("settings.json"), "r+", encoding='utf8')
    db_data = json.load(json_file)
    if json_obj == 'backups_create' or json_obj == 'latest_stencil':
        merger = db_data[json_obj]
        merger.update(data)
        db_data[json_obj] = merger
    elif json_obj == 'latest_dbs_ids':
        db_data[json_obj] = data
    json_file.seek(0)
    json.dump(db_data, json_file, ensure_ascii=False, indent=4)
    json_file.truncate()
    json_file.close()

def dump(cur_datetime, flag, config, db_names, db_all):
    global EOFFlag
    EOFFlag = False
    # Делаем бэкап (дамп) таблиц из списка в Json-файле
    logger.info('-------СТАРТ БЭКАПА БАЗ ДАННЫХ-------')

    # Данные подключения для вывода в лог
    access_data_log = {'host': config['host'], 'user': config['login'], 'password': '*скрыт*',
                       'db_names': db_names, 'client_key': config["path_to_ckc"],
                       'server_ca': config["path_to_scc"], 'client_cert': config["path_to_ccc"]}
    logger.info(f'Попытка подключения к базе данных с использованием данных: {access_data_log}')
    created_backups = []  # Множество названий проектов с флагом "L", для которых есть новая версия бэкапа

    try:
        connection = MySQLdb.connect(
            host=config['host'],
            user=config['login'],
            password=config['password'],
            db=config['fs_db'],
            ssl={'key': config["path_to_ckc"], 'ca': config["path_to_scc"], 'cert': config["path_to_ccc"]}
        )
        cursor = connection.cursor()
        cursor.execute('''
                    SELECT Project_ID, Project_Name
                    FROM projects
                    WHERE Project_Status = 'L' AND Project_Backup = '1' AND Project_Project_Type_ID = '1'
                    ''')
        dbs_for_dbbckp_tuple = cursor.fetchall()  # Массив значений (ID проекта, Имя проекта)
        dbs_ids_for_dbbckp_dict = dict((x[0], x[1]) for x in dbs_for_dbbckp_tuple)  # Словарь. ID - Название
    except Exception as e:
        logger.critical(f'Ошибка выполнения запроса к базе данных! {e}')
        logger.info('-------ФИНИШ БЭКАПА БАЗ ДАННЫХ С ОШИБКОЙ | ФАЙЛЫ УДАЛЕНЫ-------')
        handlers = logger.handlers[:]
        for handler in handlers:
            logger.removeHandler(handler)
            handler.close()
        if flag == 'manual':
            EOFFlag = True
            return
        else:
            sys.exit()

    # Цикл, перебирающий базы данных
    for database in db_names:
        if EOFFlag:
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            break

        filename = f"{cur_datetime}_db_{database}.sql" # Наименование файла бэкапа
        file = f"{config['path_to_backups']}/{filename}" # Путь к файлу дампа

        # Конфигурация соединения с БД + подключение + дамп
        # dump_cmd = f"mysqldump --user={config['login']} --password={config['password']} --skip-lock-tables --host={config['host']} {database} --ssl-ca={config['path_to_scc']} --ssl-key={config['path_to_ckc']} --ssl-cert={config['path_to_ccc']} > {file}"
        dump_cmd_list = ["mysqldump", f"--user={config['login']}", f"--password={config['password']}", "--skip-lock-tables", f"--host={config['host']}", f"{database}", f"--ssl-ca={config['path_to_scc']}", f"--ssl-key={config['path_to_ckc']}", f"--ssl-cert={config['path_to_ccc']}"]
        logger.info(f'Подключение к базе данных {database}')
        logger.info(f'Начало бэкапа базы данных {database}')
        try:
            global dump_call
            with open(file, 'w') as outfile:
                dump_call = subprocess.Popen(dump_cmd_list, stdout=outfile, shell=False)
                dump_call.communicate()
            # os.system(dump_cmd) # Дамп бд через os.system (устаревшее, ибо процесс нельзя завершить досрочно)
        except Exception as e:
            logger.critical(f'Ошибка подключения и/или бэкапа базы данных {database}! {e}')
            logger.info('-------ФИНИШ БЭКАПА БАЗ ДАННЫХ С ОШИБКОЙ | ФАЙЛЫ УДАЛЕНЫ-------')
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            if flag == 'manual':
                EOFFlag = True
                break
            else:
                sys.exit()

        if EOFFlag:
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            break

        logger.info(f'Бэкап базы данных {database} успешно создан')
        logger.info(f'Начало архивации бэкапа базы данных {database}')
        # Архивируем файл с бэкапом и удаляем исходник
        try:
            global arch_call
            tar_cmd = ['tar', '-czf', f'{file.replace(".sql", ".tar.gz")}', f'{file}']
            arch_call = subprocess.Popen(tar_cmd, shell=False)
            arch_call.communicate()
            os.remove(file)
        except Exception as e:
            logger.critical(f'Ошибка архивации файла бэкапа! {e}')
            logger.info('-------ФИНИШ БЭКАПА БАЗ ДАННЫХ С ОШИБКОЙ | ФАЙЛЫ УДАЛЕНЫ-------')
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            if flag == 'manual':
                EOFFlag = True
                break
            else:
                sys.exit()

        if EOFFlag:
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            break

        created_backups.append(database)
        logger.info(f'Бэкап базы данных {database} успешно архивирован')
        logger.info(f'Начало сборки и сохранения данных о содержимом бэкапа БД {database}')

        # Формальности и запись данных о произведённом бэкапе в БД
        try:
            connection = MySQLdb.connect(
                host=config['host'],
                user=config['login'],
                password=config['password'],
                db=config['fs_db'],
                ssl={'key': config["path_to_ckc"], 'ca': config["path_to_scc"], 'cert': config["path_to_ccc"]}
            )
            cursor = connection.cursor()

            # Получаем список списков (Название таблицы, Размер таблицы)
            cursor.execute('''SELECT TABLE_NAME, DATA_LENGTH + INDEX_LENGTH
                                    FROM information_schema.TABLES
                                    WHERE TABLE_SCHEMA = %s
                                    ORDER BY TABLE_NAME
                                    ASC;''', [database])
            all_tables_sizes = cursor.fetchall()

            # Получаем форматированное значение даты и времени последнего бэкапа, в часовой зоне UTC
            q_d = datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M') - datetime.strptime(datetime.utcnow().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M') # Сдвиг по UTC
            r_d = datetime.strptime(cur_datetime, '%Y_%m_%d_%H_%M') - q_d
            formatted_cur_datetime_tuple = str(r_d.strftime('%Y_%m_%d_%H_%M')).split('_')
            formatted_cur_datetime = f'{formatted_cur_datetime_tuple[0]}-{formatted_cur_datetime_tuple[1]}-{formatted_cur_datetime_tuple[2]} {formatted_cur_datetime_tuple[3]}:{formatted_cur_datetime_tuple[4]}'
            # Вставляем данные о бэкапе в таблицу backups
            cursor.execute('''INSERT INTO backups (`Backup_Device`, `Backup_Project_ID`, `Backup_File_Name`, `Backup_Create`, `Backup_Note`)
                            VALUES ('SUROK', %s, %s, %s, '')''', (db_all[database], cur_datetime + "_db_" + database + ".tar.gz", formatted_cur_datetime))
            connection.commit()

            if EOFFlag:
                handlers = logger.handlers[:]
                for handler in handlers:
                    logger.removeHandler(handler)
                    handler.close()
                break

            # Получаем ID последнего (нынешнего) бэкапа
            cursor.execute('''SELECT MAX(Backup_ID)
            FROM backups''')
            latest_backup_id = cursor.fetchall()
            latest_backup_id_formatted = [x[0] for x in latest_backup_id][0]
            for table_size in all_tables_sizes:
                # Добавляем данные о всех таблицах и их размере + ID нынешнего бэкапа
                cursor.execute('''INSERT INTO backup_tables (`Backup_Table_Backup_ID`, `Backup_Table_Name`, `Backup_Table_Size`)
                    VALUES (%s, %s, %s)''', (latest_backup_id_formatted, table_size[0], table_size[1]))
                connection.commit()

            logger.info(f'Данные о содержимом бэкапа БД {database} успешно собраны и сохранены')
        except Exception as e:
            logger.critical(f'Ошибка выполнения запроса к базе данных! {e}')
            logger.info('-------ФИНИШ БЭКАПА БАЗ ДАННЫХ С ОШИБКОЙ | ФАЙЛЫ УДАЛЕНЫ-------')
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            if flag == 'manual':
                EOFFlag = True
                break
            else:
                sys.exit()

    # Записываем в json-файл актуальные значения по проектам/бэкапам
    latest_stencil = {(str(x), dbs_ids_for_dbbckp_dict[x]) for x in dbs_ids_for_dbbckp_dict}
    actual_value = {(str(x), cur_datetime) for x in dbs_ids_for_dbbckp_dict}
    latest_dbs_ids = [str(x[0]) for x in dbs_for_dbbckp_tuple]
    json_adder(actual_value, 'backups_create')
    json_adder(latest_stencil, 'latest_stencil')
    json_adder(latest_dbs_ids, 'latest_dbs_ids')
    logger.info('-------ФИНИШ БЭКАПА БАЗ ДАННЫХ-------')
    # Закрываем соединение
    connection.close()

