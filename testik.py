import os
import sys
import json
import MySQLdb
from datetime import datetime
import subprocess
#
def resource_path(relative_path):
    # Преобразование путей ресурсов в пригодный для использования формат для PyInstaller
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath('.'), relative_path)

config_file = open(resource_path("settings.json"), 'r')
config = json.load(config_file)  # json-файл конфигурации
config_file.close()

connection = MySQLdb.connect(
    host=config['host'],
    user=config['login'],
    password=config['password'],
    db='monte_db_test',
    ssl={'key': config["path_to_ckc"], 'ca': config["path_to_scc"], 'cert': config["path_to_ccc"]}
)
cursor = connection.cursor()
# cursor.execute('''INSERT INTO backup_files (Backup_File_Backup_ID, Backup_File_Path, Backup_File_Name, Backup_File_Size, Backup_File_Content)
#     SELECT Backup_File_Backup_ID, Backup_File_Path, Backup_File_Name, Backup_File_Size, Backup_File_Content
#       FROM backup_files
#      WHERE Backup_File_Backup_ID = %s''', [])
# connection.commit()

cursor.execute('''OPTIMIZE TABLE addresses;''')
connection.commit()

# cursor.execute('''SELECT TABLE_NAME, DATA_LENGTH + INDEX_LENGTH
#                         FROM information_schema.TABLES
#                         WHERE TABLE_SCHEMA = %s
#                         ORDER BY TABLE_NAME
#                         ASC;''', ['monte_db_w'])
# all_tables_sizes = cursor.fetchall()
# print(all_tables_sizes)
# cursor.execute('''SELECT TABLE_NAME, DATA_LENGTH
#                         FROM information_schema.TABLES
#                         WHERE TABLE_SCHEMA = %s
#                         ORDER BY TABLE_NAME
#                         ASC;''', ['monte_db_w'])
# all_tables_sizes_q = cursor.fetchall()
# print(all_tables_sizes_q)

# cursor.execute('''
#     SELECT Project_ID, Project_Name, Project_Translit
#     FROM projects
#     WHERE Project_Status = 'L' AND Project_Project_Type_ID = '2'
#     ''')
# dbs_for_fsbckp_tuple = cursor.fetchall()  # Массив значений (ID проекта, Имя проекта, Дата последнего бэкапа проекта)
# ids_for_fsbckp_list = [x[0] for x in dbs_for_fsbckp_tuple]  # Список ID проектов из массива выше
# # Создаём аналогичный словарь из кортежа для более простого взаимодействия
# dbs_ids_for_fsbckp_dict = dict((x[0], x[1]) for x in dbs_for_fsbckp_tuple) # Словарь. ID - Название
# tdbs_ids_for_fsbckp_dict = dict((x[0], x[2]) for x in dbs_for_fsbckp_tuple)  # Словарь. ID - Название-транслит
#
# cursor.execute(f'''
#     SELECT Backup_ID, Backup_Project_ID, Backup_Create
#     FROM backups
#     WHERE Backup_Device = 'GAE' AND Backup_Download IS NULL AND Backup_Project_ID IN ({",".join(("%s",) * len(ids_for_fsbckp_list))})
#     ORDER BY Backup_ID
#     ASC''', ids_for_fsbckp_list)
# fsbckp_data = cursor.fetchall() # Кортеж всех бэкапов без флага up_to_date
# fsbckp_data_uptodate_dict = dict((x[1], (x[0], x[2])) for x in fsbckp_data)  # Словарь. ID проекта - (ID последнего бэкапа проекта, Дата и время бэкапа)
# fsbckp_data_uptodate = tuple((fsbckp_data_uptodate_dict[x][0], x, fsbckp_data_uptodate_dict[x][1]) for x in fsbckp_data_uptodate_dict)  # Кортеж со значениями из словаря выше. Создан, т.к. применение словаря для бэкапа ФС без флага up_to_date - невозможно. Ключи должны быть уникальными.
#
# fsbckp_data_to_parse = fsbckp_data_uptodate
#
# fsbckp_id_date_dict = dict((x[0], x[2]) for x in fsbckp_data_to_parse)  # Словарь актуальных данных для парсинга. ID бэкапа - Дата и время бэкапа
#
# actual_value = {}  # Если есть новая версия бэкапа проекта, в словарь добавляется ID проекта (ключ) и дата его бэкапа (значение)
# latest_project = [99999999999999999999, 'testtesttesttesttest']  # Список с ID текущего обрабатываемого бэкапа и датой первого файла этого проекта. Текущие значения - условные, для корректного начала выполнения скрипта
# created_backups = []  # Множество названий проектов с флагом "L", для которых есть новая версия бэкапа
# for backup_instance in fsbckp_data_to_parse:
#     # Цикл прохода по бэкапам. Получаем список списков с данными файлов.
#     # Создаём кортеж данных из массива значений (ID бэкапа, Путь к файлу в структуре ФС, Имя файла, Содержимое файла)
#     cursor.execute('''
#         SELECT Backup_File_Backup_ID, Backup_File_Path, Backup_File_Name, Backup_File_Content
#         FROM backup_files
#         WHERE Backup_File_Backup_ID = %s
#         ''', [backup_instance[0]])
#     fs_fsbckp_data = cursor.fetchall()
#     print(fs_fsbckp_data)

#     # Запись в переменную даты бэкапа первого файла проекта
#     latest_project[0] = backup_instance[0]
#     latest_project[1] = fsbckp_id_date_dict[backup_instance[0]]
#     actual_value[str(latest_project[0])] = str(latest_project[1]).replace('-', '_').replace(':', '_').replace(' ', '_')
#
#     for file_data in db_fsbckp_data:
#         # Цикл прохода по файлам бэкапа
#         # Воссоздаём файловую систему + записываем файлы
#         print(config["path_to_backups"] + "/" + 'test' + "(" + str(backup_instance[2]) + ")" + "_app_" + tdbs_ids_for_fsbckp_dict[backup_instance[0]] + "/" + file_data[1] + file_data[2])
#         created_backups.append(dbs_ids_for_fsbckp_dict[backup_instance[0]])
# print(actual_value)
# print(created_backups)