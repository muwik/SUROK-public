import os
import run
import sys
import json
import mail
import dbbckp
import fsbckp
import shutil
import MySQLdb
import surcron
import logging
import requests
import platform
import subprocess
from time import sleep
from requests import get
from croniter import croniter
from toggle import AnimatedToggle
from datetime import datetime, time, timedelta, date
from PyQt6.QtGui import QPixmap, QFont, QIcon, QCursor, QBrush, QColor, QMovie
from PyQt6.QtCore import QDateTime, Qt, QTimer, QRect, QSize, QTime, QObject, QThread, pyqtSignal
from PyQt6.QtWidgets import QApplication, QCheckBox, QComboBox, QTextEdit, QGridLayout, QVBoxLayout, QHBoxLayout, QGroupBox, \
    QLabel, QLineEdit, QPushButton, QSizePolicy, QSpinBox, QTableWidget, QTabWidget, QWidget, QStatusBar, QMainWindow, \
    QFileDialog, QMessageBox, QMenu

def resource_path(relative_path):
    # Преобразование путей ресурсов в пригодный для использования формат для PyInstaller
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath('.'), relative_path)

class Singleton(type(QObject), type):
    # Класс Синглтон - позволяет избежать повторную инициализацию класса (окна, в моём случае)
    def __init__(cls, name, bases, dict):
        super().__init__(name, bases, dict)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance

class ConsoleWindowLogHandler(logging.Handler, QObject):
    # Лог-хэндлер - получает и преобразует поток логов
    sigLog = pyqtSignal(str)
    def __init__(self):
        logging.Handler.__init__(self)
        QObject.__init__(self)

    def emit(self, logRecord):
        message = self.format(logRecord)
        self.sigLog.emit(message)

class CustomFormatter(logging.Formatter):
    # Преобразователь логов в цветной текст: выделяет ошибки и предупреждения
    black = '<span style="color:Black;">'
    yellow = '<span style="color:Orange;">'
    red = '<span style="color:OrangeRed;">'
    bold_red = '<span style="color:Crimson;">'
    reset = '</span>'
    format = '%(asctime)s %(levelname)s %(message)s'

    FORMATS = {
        logging.DEBUG: black + format + reset,
        logging.INFO: black + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%d.%m.%Y %H:%M:%S')
        return formatter.format(record)

class Diagnostics(QWidget, metaclass=Singleton):
    # Класс интерфейса окна диагностики
    def __init__(self):
        super().__init__()

        # Флаг возникновения первой ошибки
        self.error_flag = False

        # Установка данного окна, как единственного модального. (Блокировка остальных окон)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        # Настройки окна диагностики
        self.setFixedSize(QSize(380, 280))
        self.dg_layout = QGridLayout()
        self.setLayout(self.dg_layout)
        self.dg_layout.setSpacing(0)

        # Заголовок
        self.label = QLabel("Диагностика:")
        self.label.setToolTip("Дигностика И-нета, б.БД, б.ФС и Почты")
        self.l_font = self.label.font()
        self.l_font.setWeight(500)
        self.label.setFont(self.l_font)

        # Кнопка "Проверить"
        self.ok_btn = QPushButton("Проверить")

        # Иконка диагностики интернет-соединения
        self.label_di_nw = QLabel(self)
        self.label_di_nw.setText("")
        self.label_di_nw.setHidden(True)

        # Иконка диагностики соединения с БД для б.БД
        self.label_di_db = QLabel(self)
        self.label_di_db.setText("")
        self.label_di_db.setHidden(True)

        # Иконка диагностики соединения с БД для б.ФС
        self.label_di_fs = QLabel(self)
        self.label_di_fs.setText("")
        self.label_di_fs.setHidden(True)

        # Иконка диагностики соединения с почтовым сервером
        self.label_di_ml = QLabel(self)
        self.label_di_ml.setText("")
        self.label_di_ml.setHidden(True)

        # Поле для вывода ошибок
        self.errors_textbox = QTextEdit(self)
        self.errors_textbox.setReadOnly(True)
        self.errors_textbox.setMaximumHeight(110)
        self.errors_textbox.setObjectName('ErrorTextBox')

        # Добавляем виджеты в макет окна
        self.dg_layout.addWidget(self.label, 0, 0, 1, 2, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.dg_layout.addWidget(QLabel("Интернет-соединение"), 1, 0)
        self.dg_layout.addWidget(QLabel("Соединение с БД для б.БД"), 2, 0)
        self.dg_layout.addWidget(QLabel("Соединение с БД для б.ФС"), 3, 0)
        self.dg_layout.addWidget(QLabel("Соединение с почтовым сервером"), 4, 0)
        self.dg_layout.addWidget(self.errors_textbox, 5, 0, 1, 2)

        # Изображения иконок: зелёная и красная
        self.pixmap_yes = QPixmap(resource_path("assets/di-g.png")).scaled(12, 12, Qt.AspectRatioMode.KeepAspectRatio,
                                                         Qt.TransformationMode.FastTransformation)

        self.pixmap_no = QPixmap(resource_path("assets/di-r.png")).scaled(12, 12, Qt.AspectRatioMode.KeepAspectRatio,
                                                      Qt.TransformationMode.FastTransformation)

    def nw_con(self):
        # Тест интернет-соединения
        url = "https://www.google.com/"
        timeout = 5
        sleep(0.1)
        try:
            request = requests.get(url, timeout=timeout)
            self.label_di_nw.setPixmap(self.pixmap_yes)
            self.dg_layout.addWidget(self.label_di_nw, 1, 1, alignment=Qt.AlignmentFlag.AlignRight)
        except Exception as e:
            self.label_di_nw.setPixmap(self.pixmap_no)
            self.dg_layout.addWidget(self.label_di_nw, 1, 1, alignment=Qt.AlignmentFlag.AlignRight)
            self.error_flag = True
            self.errors_textbox.append(f'И-нет (Тестовый сайт: {url}) -> {e}')
        self.label_di_nw.setHidden(False)

    def db_con(self):
        # Тест БД соединения для б.БД
        config = SUROK_Admin().json_adder()
        sleep(0.1)
        error_backups =[]
        if SUROK_Admin().db_getter()[0] == ():
            error_backups.append("NO DB")
            self.error_flag = True
            self.errors_textbox.append(f'б.БД -> No dbs povided for diagnostics')
        for database in SUROK_Admin().db_getter()[0]:
            try:
                connection = MySQLdb.connect(
                    host=config['host'],
                    user=config['login'],
                    password=config['password'],
                    db=database,
                    ssl={'key': config["path_to_ckc"], 'ca': config["path_to_scc"], 'cert': config["path_to_ccc"]}
                )
                cursor = connection.cursor()
                cursor.execute("SELECT VERSION()")
                results = cursor.fetchone()
                connection.close()
                if not results:
                    error_backups.append(database)
                    self.error_flag = True
                    self.errors_textbox.append(f'б.БД -> {database} -> Test request execution error')
            except Exception as e:
                error_backups.append(database)
                self.error_flag = True
                self.errors_textbox.append(f'б.БД -> {database} -> {e}')
        if not error_backups:
            self.label_di_db.setPixmap(self.pixmap_yes)
            self.dg_layout.addWidget(self.label_di_db, 2, 1, alignment=Qt.AlignmentFlag.AlignRight)
        else:
            self.label_di_db.setPixmap(self.pixmap_no)
            self.dg_layout.addWidget(self.label_di_db, 2, 1, alignment=Qt.AlignmentFlag.AlignRight)
        self.label_di_db.setHidden(False)

    def fs_con(self):
        # Тест БД соединения для б.ФС
        config = SUROK_Admin().json_adder()
        sleep(0.1)
        try:
            connection = MySQLdb.connect(
                host=config['host'],
                user=config['login'],
                password=config['password'],
                db=config['fs_db'],
                ssl={'key': config["path_to_ckc"], 'ca': config["path_to_scc"], 'cert': config["path_to_ccc"]}
            )
            cursor = connection.cursor()
            cursor.execute("SELECT VERSION()")
            results = cursor.fetchone()
            connection.close()
            if results:
                self.label_di_fs.setPixmap(self.pixmap_yes)
                self.dg_layout.addWidget(self.label_di_fs, 3, 1, alignment=Qt.AlignmentFlag.AlignRight)
            else:
                self.label_di_fs.setPixmap(self.pixmap_no)
                self.dg_layout.addWidget(self.label_di_fs, 3, 1, alignment=Qt.AlignmentFlag.AlignRight)
                self.error_flag = True
                self.errors_textbox.append(f'б.ФС -> Test request execution error')
        except Exception as e:
            self.label_di_fs.setPixmap(self.pixmap_no)
            self.dg_layout.addWidget(self.label_di_fs, 3, 1, alignment=Qt.AlignmentFlag.AlignRight)
            self.error_flag = True
            self.errors_textbox.append(f'б.ФС -> {e}')
        self.label_di_fs.setHidden(False)

    def ml_con(self):
        # Тест соединения с почтовым сервером
        sleep(0.1)
        try:
            mail.diag()
            self.label_di_ml.setPixmap(self.pixmap_yes)
            self.dg_layout.addWidget(self.label_di_ml, 4, 1, alignment=Qt.AlignmentFlag.AlignRight)
        except Exception as e:
            self.label_di_ml.setPixmap(self.pixmap_no)
            self.dg_layout.addWidget(self.label_di_ml, 4, 1, alignment=Qt.AlignmentFlag.AlignRight)
            self.error_flag = True
            self.errors_textbox.append(f'Почтовый сервер -> {e}')
        self.label_di_ml.setHidden(False)
        if not self.error_flag:
            self.errors_textbox.append('--ОШИБОК НЕТ--')

    def closeEvent(self, event):
        # Сценарий закрытия окна
        self.error_flag = False
        self.errors_textbox.clear()
        self.label_di_nw.setPixmap(QPixmap())
        self.label_di_db.setPixmap(QPixmap())
        self.label_di_fs.setPixmap(QPixmap())
        self.label_di_ml.setPixmap(QPixmap())

class Last_backup_info(QWidget, metaclass=Singleton):
    # Класс интерфейса окна информации о последнем бэкапе
    def __init__(self):
        super().__init__()

        # Установка данного окна, как единственного модального. (Блокировка остальных окон)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        # Настройки окна выбора бэкапа
        self.setFixedWidth(480)
        self.bi_layout = QGridLayout()
        self.bi_layout.setSpacing(5)
        self.bi_layout.setColumnMinimumWidth(0, 180)
        self.bi_layout.setColumnMinimumWidth(1, 120)
        self.bi_layout.setRowStretch(0, 2)
        self.setLayout(self.bi_layout)

        # Заголовок
        self.label = QLabel("Даты последних версий бэкапов (ФС - UTC, ДБ - Локал.):")
        self.l_font = self.label.font()
        self.l_font.setWeight(500)
        self.label.setFont(self.l_font)
        self.label.setFixedHeight(40)

        # Изображения иконка-индикаторов наличия/отсутствия актуальной версии бэкапа
        self.pixmap_act_a = QPixmap(resource_path("assets/a-act.png")).scaled(20, 20, Qt.AspectRatioMode.KeepAspectRatio,
                                                                            Qt.TransformationMode.FastTransformation)
        self.pixmap_act_o = QPixmap(resource_path("assets/o-act.png")).scaled(20, 20, Qt.AspectRatioMode.KeepAspectRatio,
                                                                            Qt.TransformationMode.FastTransformation)

    def add_last_backups_data(self):
        # Очищаем таблицу от прежних значений
        for i in reversed(range(self.bi_layout.count())):
            self.bi_layout.itemAt(i).widget().setParent(None)

        # Добавляем виджеты в макет окна
        self.bi_layout.addWidget(self.label, 0, 0, 1, 3, alignment=Qt.AlignmentFlag.AlignCenter)
        projects = SUROK_Admin().json_adder()['backups_create']
        stencil = SUROK_Admin().json_adder()['latest_stencil']
        dbs_ids = SUROK_Admin().json_adder()['latest_dbs_ids']
        fss_ids = SUROK_Admin().json_adder()['latest_fss_ids']

        for i, project in enumerate(projects):
            self.bi_layout.addWidget(QLabel(stencil[project]), i+1, 0)
            self.bi_layout.addWidget(QLabel(projects[project]), i+1, 1)
            if project in dbs_ids or project in fss_ids:
                label_act_a = QLabel(self)
                label_act_a.setText("")
                label_act_a.setPixmap(self.pixmap_act_a)
                self.bi_layout.addWidget(label_act_a, i+1, 2)
            else:
                label_act_o = QLabel(self)
                label_act_o.setText("")
                label_act_o.setPixmap(self.pixmap_act_o)
                self.bi_layout.addWidget(label_act_o, i+1, 2)

class Choose_backup(QWidget):
    # Класс интерфейса окна информации о последнем бэкапе
    def __init__(self):
        super().__init__()

        # Установка данного окна, как единственного модального. (Блокировка остальных окон)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        # Инициализация окна запуска бэкапов как изначально закрытого
        self.b_con = None

        # Настройки окна выбора бэкапа
        self.setFixedSize(QSize(380, 190))
        self.ch_layout = QVBoxLayout()
        self.setLayout(self.ch_layout)

        # Заголовок
        self.label = QLabel("Выберите, какой backup хотите сделать:")
        self.l_font = self.label.font()
        self.l_font.setWeight(500)
        self.label.setFont(self.l_font)

        # Чекбоксы выбора бэкапов
        self.dbb = QCheckBox("Backup баз данных")
        self.fsb = QCheckBox("Backup файловой системы")
        self.dbb.setChecked(False)
        self.fsb.setChecked(False)

        # Кнопка "ОК"
        self.ok_btn = QPushButton("OK")
        self.ok_btn.clicked.connect(lambda: self.check_state(self.dbb.isChecked(), self.fsb.isChecked()))

        # Подсказка внизу окна
        self.ok_tip = QLabel("После нажатия на кнопку потребуется дополнительное подтверждение")
        self.ok_tip.setObjectName("tooltip")
        self.ok_tip.setMaximumHeight(10)

        # Добавляем виджеты в макет окна
        self.ch_layout.addWidget(self.label, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.ch_layout.addWidget(self.dbb, alignment=Qt.AlignmentFlag.AlignLeft)
        self.ch_layout.addWidget(self.fsb, alignment=Qt.AlignmentFlag.AlignLeft)
        self.ch_layout.addWidget(self.ok_btn, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.ch_layout.addWidget(self.ok_tip, alignment=Qt.AlignmentFlag.AlignHCenter)

    def check_state(self, db_flag, fs_flag):
        # Считывание состояния чекбоксов и отправки значений в следующее окно
        if db_flag or fs_flag:
            self.open_bckp_confirmation(db_flag, fs_flag) # Открываем окно выбора бэкапов
        else:
            # Окно предупреждения, в случае невыбора ни одного чекбокса
            self.messageC = QMessageBox()
            self.messageC.setIcon(QMessageBox.Icon.Warning)
            self.messageC.setInformativeText("Выберите хотя-бы один бэкап!")
            self.view = self.messageC.exec()

    def open_bckp_confirmation(self, db_flag, fs_flag):
        # Отображение окна запуска бэкапа
        self.b_con = Confirm_backup(db_flag, fs_flag)
        self.close()
        self.b_con.show()

class Confirm_backup(QWidget):
    # Класс интерфейса окна запуска бэкапа
    def __init__(self, db_flag, fs_flag):
        super().__init__()

        # Установка данного окна, как единственного модального. (Блокировка остальных окон)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)

        # Полученные флаги-значения чекбоксов
        self.db_flag = db_flag
        self.fs_flag = fs_flag

        # Определение значения сценария бэкапа, согласно значениям чекбоксов
        self.scenario = 0
        if self.db_flag and not self.fs_flag:
            self.scenario = 1
        elif not self.db_flag and self.fs_flag:
            self.scenario = 2
        else:
            self.scenario = 3

        # Настройки окна запуска бэкапа
        self.setFixedSize(QSize(670, 476))
        self.strt_layout = QVBoxLayout()
        self.setLayout(self.strt_layout)

        # Контрольный лист (предупреждения, уведомления)
        self.label = QLabel("Контрольный лист:")
        self.l_font = self.label.font()
        self.l_font.setWeight(500)
        self.label.setFont(self.l_font)

        # Макет контрольного листа
        self.war_layout = QGridLayout()
        self.war_layout.setContentsMargins(0, 0, 0, 0)
        self.war_layout.setSpacing(10)

        # Лейблы предупреждений
        self.cron_warn = QLabel()
        self.cron_warn_d = QLabel()
        self.db_bckp_warn = QLabel()
        self.fs_bckp_warn = QLabel()
        self.fs_bckp_ignr = QLabel()

        # Иконка предупреждения CRON
        self.label_wr_cron = QLabel(self)
        self.label_wr_cron.setText("")

        # Иконка предупреждения БД
        self.label_wr_db = QLabel(self)
        self.label_wr_db.setText("")

        # Иконка предупреждения ФС
        self.label_wr_fs = QLabel(self)
        self.label_wr_fs.setText("")

        # Иконка скачивания актуальных версий бэкапов ФС
        self.label_ig_fs = QLabel(self)
        self.label_ig_fs.setText("")

        # Тумблер активации скачивания лишь актуальных версий бэкапа
        self.tmblr_ig_fs = AnimatedToggle(checked_color="#681889")
        self.tmblr_ig_fs.bar_checked_brush = QBrush(QColor('#b29eba'))
        self.tmblr_ig_fs.setFixedSize(QSize(38, 25))

        # Изображения иконок предупреждений, уведомлений
        self.pixmap_wr_g = QPixmap(resource_path("assets/wr-g.png")).scaled(20, 20, Qt.AspectRatioMode.KeepAspectRatio,
                                                          Qt.TransformationMode.FastTransformation)
        self.pixmap_wr_y = QPixmap(resource_path("assets/wr-y.png")).scaled(20, 20, Qt.AspectRatioMode.KeepAspectRatio,
                                                          Qt.TransformationMode.FastTransformation)
        self.pixmap_wr_b = QPixmap(resource_path("assets/wr-b.png")).scaled(20, 20, Qt.AspectRatioMode.KeepAspectRatio,
                                                          Qt.TransformationMode.FastTransformation)
        self.pixmap_wr_r = QPixmap(resource_path("assets/wr-r.png")).scaled(20, 20, Qt.AspectRatioMode.KeepAspectRatio,
                                                          Qt.TransformationMode.FastTransformation)
        self.pixmap_wr_p = QPixmap(resource_path("assets/wr-p.png")).scaled(20, 20, Qt.AspectRatioMode.KeepAspectRatio,
                                                          Qt.TransformationMode.FastTransformation)

        # Инициализация CRON-предупреждения, с условиями
        if not SUROK_Admin().json_adder()['cron']:
            self.cron_warn.setText('Запланированных сценариев CRON - нет. Накладки и конфликты исключены.')
            self.label_wr_cron.setPixmap(self.pixmap_wr_g)
            self.war_layout.addWidget(self.cron_warn, 0, 1)
        else:
            self.cron_warn.setText('Запланирован сценарий CRON через: ')
            self.cron_warn_d.setText("CRON откл. на время backup'a")

            # Макет ряда с лейблом обратного отсчёта из соседнего класса
            SUROK_Admin().cron_countdown_job_brother.setHidden(False)
            self.cw_cntdwn_row = QHBoxLayout()
            self.cw_cntdwn_row.addWidget(self.cron_warn)
            self.cw_cntdwn_row.addWidget(SUROK_Admin().cron_countdown_job_brother)
            self.cw_cntdwn_row.addWidget(self.cron_warn_d)
            self.cw_cntdwn_row.setContentsMargins(0, 0, 0, 0)
            self.cw_cntdwn_row.setSpacing(0)

            # Виджет ряда с лейблом обратного отсчёта из соседнего класса
            self.cw_cntdwn_wdgt = QWidget(self)
            self.cw_cntdwn_wdgt.setLayout(self.cw_cntdwn_row)

            self.label_wr_cron.setPixmap(self.pixmap_wr_y)
            self.war_layout.addWidget(self.cw_cntdwn_wdgt, 0, 1, alignment=Qt.AlignmentFlag.AlignLeft)
        self.war_layout.addWidget(self.label_wr_cron, 0, 0)

        # Инициализация БД-предупреждения, с условиями
        if self.db_flag:
            self.db_bckp_warn.setText('Выбран backup баз данных. Убедитесь, что запускаете процедуру в нерабочее время.')
            self.label_wr_db.setPixmap(self.pixmap_wr_b)
            self.war_layout.addWidget(self.label_wr_db, 1, 0)
            self.war_layout.addWidget(self.db_bckp_warn, 1, 1)

        # Инициализация ФС-предупреждения, с условиями
        if self.fs_flag:
            self.fs_bckp_warn.setText("Выбран backup файловой системы. Убедитесь в наличии новых версий backup'ов.")
            self.fs_bckp_ignr.setText("Сохранить лишь последние версии бэкапов ФС?")

            self.fs_bckp_dash = QLabel()
            self.fs_bckp_dash.setText('<font color="grey">------------------</font>')
            self.fs_bckp_dash.setAlignment(Qt.AlignmentFlag.AlignCenter)

            # Макет ряда с предупреждением и тумблером
            self.warn_tmblr_row = QHBoxLayout()
            self.warn_tmblr_row.setContentsMargins(0, 0, 0, 0)
            self.warn_tmblr_row.setSpacing(0)
            self.warn_tmblr_row.addWidget(self.fs_bckp_ignr)
            self.warn_tmblr_row.addWidget(self.fs_bckp_dash)
            self.warn_tmblr_row.addWidget(self.tmblr_ig_fs)

            # Виджет ряда с предупреждением и тумблером
            self.warn_tmblr_wdgt = QWidget(self)
            self.warn_tmblr_wdgt.setLayout(self.warn_tmblr_row)

            self.label_wr_fs.setPixmap(self.pixmap_wr_b)
            self.label_ig_fs.setPixmap(self.pixmap_wr_p)
            self.war_layout.addWidget(self.label_wr_fs, 2, 0)
            self.war_layout.addWidget(self.fs_bckp_warn, 2, 1)
            self.war_layout.addWidget(self.label_ig_fs, 3, 0)
            self.war_layout.addWidget(self.warn_tmblr_wdgt, 3, 1)

        # Виджет с макетом контрольного листа
        self.warnings = QWidget()
        self.warnings.setLayout(self.war_layout)

        # Кнопка запуска сценария бэкапа
        self.go_btn = QPushButton("Начать")
        self.go_btn.clicked.connect(self.date_check)
        self.go_btn.setMinimumWidth(90)

        # Кнопка останова сценария бэкапа
        self.st_btn = QPushButton("Прервать")
        self.st_btn.setDisabled(True)
        self.st_btn.clicked.connect(self.manual_stp)
        self.st_btn.setMinimumWidth(90)

        # Макет ряда с двумя кнопками
        self.btn_row = QHBoxLayout()
        self.btn_row.addWidget(self.go_btn)
        self.btn_row.addWidget(self.st_btn)
        self.btn_row.setContentsMargins(0, 0, 0, 0)
        self.btn_row.setSpacing(5)

        # Виджет ряда с двумя кнопками
        self.btn_wdgt = QWidget(self)
        self.btn_wdgt.setLayout(self.btn_row)

        # Окно вывода логов
        self.log_textbox = QTextEdit()
        self.log_textbox.setReadOnly(True)
        self.log_textbox.setMinimumWidth(600)

        # Макет окна вывода логов
        self.log_layout = QVBoxLayout()
        self.log_layout.addWidget(self.log_textbox)

        # Груп-бокс вывода логов
        self.log_log = QGroupBox(self)
        self.log_log.setTitle("Лог прогресса")
        self.log_log.setLayout(self.log_layout)

        # Поток для вывода логов
        self.bee = Worker(self.manual_strt, ())
        self.bee.finished.connect(self.restoreUi)

        # Настройки логгера и его хэндлера
        self.logger = logging.getLogger('logger')
        self.consoleHandler = ConsoleWindowLogHandler()
        self.consoleHandler.sigLog.connect(self.log_textbox.append)
        self.consoleHandler.setFormatter(CustomFormatter())

        # Добавляем виджеты в макет окна
        self.strt_layout.addWidget(self.label, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.strt_layout.addWidget(self.warnings, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.strt_layout.addWidget(self.btn_wdgt, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.strt_layout.addWidget(self.log_log, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.strt_layout.setSpacing(10)

        # Окно предупреждения о запуске бэкапа раз в минуту
        self.messageW = QMessageBox()
        self.messageW.setIcon(QMessageBox.Icon.Warning)
        self.messageW.setInformativeText("Запуск бэкапа возможен не чаще чем раз в минуту!")

        # Окно подтверждения остановки сценария бэкапа
        self.messageS = QMessageBox()
        self.messageS.setIcon(QMessageBox.Icon.Question)
        self.messageS.setInformativeText("Вы действительно хотите прервать сценарий бэкапа? Все файлы этой сессии будут удалены!")
        self.messageS.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)

        # Окно уведомления об остановке сценария бэкапа и удалении файлов
        self.messageI = QMessageBox()
        self.messageI.setIcon(QMessageBox.Icon.Information)
        self.messageI.setInformativeText("Сценарий прерван. Файлы удалены.")

        # Окно уведомления о неправильной настройке почтовой отправки логов
        self.messageME = QMessageBox()
        self.messageME.setIcon(QMessageBox.Icon.Critical)
        self.messageME.setInformativeText("Ошибка отправки письма! Запустите диагностику и проверьте правильность написания адреса(ов)!")

        self.tmblr_cron_flag = None # Флаг положения тумблера CRON'a перед бэкапом

    def date_check(self):
        # Проверка даты последнего запуска, во избежание записи в лог предыдушего запуска
        if SUROK_Admin().json_adder()['latest_run'] != datetime.now().strftime(SUROK_Admin().json_adder()['dateFormat']):
            # Запуск потока для логгирования
            self.tmblr_cron_flag = SUROK_Admin().tmblr_cron.isChecked()
            SUROK_Admin().tmblr_cron.setChecked(False)
            self.log_textbox.clear()
            self.go_btn.setEnabled(False)
            self.st_btn.setDisabled(False)
            self.logger.addHandler(self.consoleHandler)
            self.bee.start()
        else:
            # Окно предупреждения о частом запуске
            self.view = self.messageW.exec()

    def manual_strt(self):
        # Ручной старт бэкапа
        try:
            run.manual_start(self.scenario, db_tuple=SUROK_Admin().db_getter()[0], db_combo=SUROK_Admin().db_getter()[1], ignr_crt=self.tmblr_ig_fs.isChecked()) # Ручной запуск бэкапа
        except Exception:
            pass
        check_log_file = open(SUROK_Admin().json_adder()['path_to_backups'] + "/logs/" + SUROK_Admin().json_adder()['latest_run'] + ".log", 'r')
        check_log = check_log_file.read()
        check_log_file.close()
        if SUROK_Admin().json_adder()['mail_alerts']:
            if 'WARNING' in check_log or 'ERROR' in check_log or 'CRITICAL' in check_log:
                try:
                    mail.run()  # Отсылаем отчёт письмом
                except Exception:
                    view = self.messageME.show()
        if dbbckp.EOFFlag or fsbckp.EOFFlag:
            try:
                self.delete_db_bckp_data()
            except Exception as e:
                self.logger.error(f'P.S. Ошибка удаления записи о бэкапах в БД {e}')
                self.logger.info('-------ПРЕРЫВАНИЕ УДАЛЕНИЯ ДАННЫХ О БЭКАПЕ С ОШИБКОЙ-------')
        if self.tmblr_cron_flag:
            SUROK_Admin().tmblr_cron.setChecked(True)
        SUROK_Admin().update_edit_line()

    def delete_db_bckp_data(self):
        handlers = self.logger.handlers[:]
        for handler in handlers:
            self.logger.removeHandler(handler)
            handler.close()
        self.logger.addHandler(self.consoleHandler)
        fh = logging.FileHandler(
            f"{SUROK_Admin().json_adder()['path_to_backups']}/logs/{SUROK_Admin().json_adder()['latest_run']}.log",
            mode='a', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%d.%m.%Y %H:%M:%S'))
        self.logger.addHandler(fh)

        # 1. Удаляем данные о бэкапе из таблицы backup_tables, по полученным ID бэкапов, относительно времени последнего бэкапа
        # 2. Удаляем данные о бэкапе из таблицы backups, от имени второго бэкапера
        # Получаем форматированное значение даты и времени последнего бэкапа, в часовой зоне UTC
        q_d = datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M') - datetime.strptime(
            datetime.utcnow().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M')  # Сдвиг по UTC
        r_d = datetime.strptime(SUROK_Admin().json_adder()["latest_run"], '%Y_%m_%d_%H_%M') - q_d
        formatted_cur_datetime_tuple = str(r_d.strftime('%Y_%m_%d_%H_%M')).split('_')
        formatted_cur_datetime = f'{formatted_cur_datetime_tuple[0]}-{formatted_cur_datetime_tuple[1]}-{formatted_cur_datetime_tuple[2]} {formatted_cur_datetime_tuple[3]}:{formatted_cur_datetime_tuple[4]}'

        connection = MySQLdb.connect(
            host=SUROK_Admin().json_adder()['host'],
            user=SUROK_Admin().json_adder()['login'],
            password=SUROK_Admin().json_adder()['password'],
            db=SUROK_Admin().json_adder()['fs_db'],
            ssl={'key': SUROK_Admin().json_adder()["path_to_ckc"],
                 'ca': SUROK_Admin().json_adder()["path_to_scc"],
                 'cert': SUROK_Admin().json_adder()["path_to_ccc"]}
        )
        cursor = connection.cursor()
        # Получаем ID всех бэкапов, где время создания бэкапа совпадает с форматированным значением нашего последнего бэкапа
        cursor.execute('''SELECT Backup_ID
                                    FROM backups
                                    WHERE Backup_Create = %s''', [formatted_cur_datetime])
        cur_backups_ids = cursor.fetchall()
        formatted_cur_backups_ids = [x[0] for x in cur_backups_ids]

        for id in formatted_cur_backups_ids:
            # Удаляем записи о всех бэкапах, где ID  совпадают с полученными выше
            cursor.execute('''DELETE FROM backup_tables
                                        WHERE Backup_Table_Backup_ID=%s;''', [id])
            connection.commit()
            cursor.execute('''DELETE FROM backup_files
                                                WHERE Backup_File_Backup_ID=%s;''', [id])
            connection.commit()

        # Удаляем записи из таблицы backups, где время создания бэкапа совпадает с форматированным значением нашего последнего бэкапа
        cursor.execute('''DELETE FROM backups
                        WHERE Backup_Create=%s;''', [formatted_cur_datetime])
        connection.commit()
        connection.close()
        dbbckp.EOFFlag = False
        fsbckp.EOFFlag = False


        handlers = self.logger.handlers[:]
        for handler in handlers:
            self.logger.removeHandler(handler)
            handler.close()
        self.delete_files_stp()

    def manual_stp(self):
        # Ручной останов бэкапа
        self.stp_que = self.messageS.exec()
        if self.stp_que == QMessageBox.StandardButton.Yes:
            if self.scenario == 1:
                dbbckp.EOFFlag = True
                try:
                    if not dbbckp.dump_call.poll():
                        dbbckp.dump_call.kill()
                except AttributeError:
                    pass
                try:
                    if not dbbckp.arch_call.poll():
                        dbbckp.arch_call.kill()
                except AttributeError:
                    pass
            elif self.scenario == 3:
                dbbckp.EOFFlag = True
                try:
                    if not dbbckp.dump_call.poll():
                        dbbckp.dump_call.kill()
                except AttributeError:
                    pass
                try:
                    if not dbbckp.arch_call.poll():
                        dbbckp.arch_call.kill()
                except AttributeError:
                    pass
                fsbckp.EOFFlag = True
            else:
                fsbckp.EOFFlag = True
            self.bee.wait()
            try:
                self.delete_db_bckp_data()
                self.logger.info('-------БЭКАП ПРЕРВАН | ФАЙЛЫ УДАЛЕНЫ-------')
            except Exception as e:
                self.logger.error(f'P.S. Ошибка удаления записи о бэкапах в БД {e}')
                self.logger.info('-------ПРЕРЫВАНИЕ БЭКАПА С ОШИБКОЙ-------')
            handlers = self.logger.handlers[:]
            for handler in handlers:
                self.logger.removeHandler(handler)
                handler.close()
            self.delete_files_stp()
            # При наличии ошибок в отчёте, высылаем его письмом по почте
            check_log_file = open(SUROK_Admin().json_adder()['path_to_backups'] + "/logs/" + SUROK_Admin().json_adder()['latest_run'] + ".log", 'r')
            check_log = check_log_file.read()
            check_log_file.close()
            if SUROK_Admin().json_adder()['mail_alerts']:
                if 'WARNING' in check_log or 'ERROR' in check_log or 'CRITICAL' in check_log:
                    try:
                        mail.run()  # Отсылаем отчёт письмом
                    except Exception:
                        view = self.messageME.show()
            self.stp_inf = self.messageI.exec()
            # Возвращаем крон во вкл. состояние, если он был вк. до запуска сценария бэкапа
            if self.tmblr_cron_flag:
                SUROK_Admin().tmblr_cron.setChecked(True)
            SUROK_Admin().update_edit_line()

    def restoreUi(self):
        # Восстановление кнопки "Начать" после завершения сценария бэкапа
        self.go_btn.setEnabled(True)
        self.st_btn.setDisabled(True)

    @staticmethod
    def delete_files_stp():
        # Удаление файлов прерванного бэкапа
        my_dir = SUROK_Admin().json_adder()['path_to_backups']
        for fname in os.listdir(my_dir):
            if fname.startswith(SUROK_Admin().json_adder()['latest_run']):
                try:
                    os.remove(os.path.join(my_dir, fname))
                except Exception:
                    shutil.rmtree(os.path.join(my_dir, fname))

    def closeEvent(self, event):
        # Сценарий закрытия окна
        if self.bee.isRunning():
            self.manual_stp()
            event.ignore()
        else:
            event.accept()

class Worker(QThread):
    # Поток для работы окна вывода логов
    def __init__(self, func, args):
        super(Worker, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.func(*self.args)

class SUROK_Admin(QMainWindow, metaclass=Singleton):
    # Класс интерфейса главного окна СУРКА
    def __init__(self):
        super().__init__()

        # Инициализация окна выбора бэкапов как изначально закрытого
        self.b_sel = Choose_backup()
        self.b_sel.close()

        # Настройки главного окна
        self.setWindowTitle('ПО "СУРОК"')
        self.setFixedSize(QSize(770, 576))

        # Список изменений для текущего сеанса
        self.message_list = []
        # Старое изменяемое значение
        self.old_json_value = None
        # Новое изменяемое значение
        self.new_json_value = None

        # Хеадер
        self.headerWidget = QTableWidget(self)
        self.headerWidget.setGeometry(QRect(0, 0, 770, 50))
        self.headerWidget.setStyleSheet("background-color: rgb(0, 105, 89);")

        # Логотип
        self.label_logo = QLabel(self)
        self.pixmap_logo = QPixmap(resource_path("assets/logo.png")).scaled(123, 38, Qt.AspectRatioMode.KeepAspectRatio,
                                                        Qt.TransformationMode.SmoothTransformation)
        self.label_logo.setGeometry(QRect(10, 6, 123, 38))
        self.label_logo.setText("")
        self.label_logo.setPixmap(self.pixmap_logo)
        self.label_logo.setToolTip(f'"Система управления резервным объёмным копированием v.1.3.0 - © {date.today().year} IvNoch"')

        # Кнопка диагностики
        self.label_trblshtng = QPushButton(self)
        self.label_trblshtng.setGeometry(QRect(605, 10, 100, 30))
        self.label_trblshtng.setIconSize(QSize(30, 30))
        self.label_trblshtng.setText("Диагностика")
        self.label_trblshtng.setObjectName('trblshtng')
        self.label_trblshtng.clicked.connect(self.open_diagnostics)

        # Кнопка уведомлений
        self.label_alerts = QPushButton(self)
        self.label_alerts.setGeometry(QRect(715, 10, 40, 30))
        if self.json_adder()['mail_alerts']:
            self.label_alerts.setIcon(QIcon(resource_path('assets/alert-on.png')))
        else:
            self.label_alerts.setIcon(QIcon(resource_path('assets/alert-off.png')))
        self.label_alerts.setIconSize(QSize(30, 30))
        self.label_alerts.setObjectName('log_btn')
        self.label_alerts.clicked.connect(self.set_alerts)

        # Макет и наполнение таблицы внутри журнала изменений
        self.layout_log = QGridLayout()
        self.layout_log.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignBottom)
        self.layout_log.setColumnMinimumWidth(0, 70)
        self.layout_log.setColumnMinimumWidth(1, 546)

        # Груп-бокс журнала изменений
        self.change_log = QGroupBox(self)
        self.change_log.setTitle("Журнал изменений")
        self.change_log.setLayout(self.layout_log)

        # Кнопка ручного бэкапа
        self.manual_strt_btn = QPushButton(self)
        self.manual_strt_btn.setText('Сделать backup сейчас')
        self.manual_strt_btn.clicked.connect(self.open_bckp_selection)

        # Дата последнего запуска
        self.last_launch = QLabel(self)
        self.last_launch.setText(f'Последний запуск: {self.json_adder()["latest_run"]}<span style="color:Grey;">⠀-</span>')
        self.last_launch.setToolTip("Последний запуск СУРКА: как ручной, так и CRON")
        self.last_launch.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.last_launch.setCursor(QCursor(Qt.CursorShape.IBeamCursor))

        # Данные работы последнего запуска
        self.latest_backups_info = QPushButton(self)
        self.latest_backups_info.setIcon(QIcon(resource_path('assets/i-info.png')))
        self.latest_backups_info.setIconSize(QSize(18, 18))
        self.latest_backups_info.setFixedWidth(28)
        self.latest_backups_info.setFixedHeight(44)
        self.latest_backups_info.setObjectName('log_btn')
        self.latest_backups_info.clicked.connect(self.open_latest_backups_info)

        # Кнопка разворачивания поля последнего лога
        self.last_log_btn = QPushButton(self)
        self.last_log_btn.setIcon(QIcon(resource_path('assets/arrow-d.png')))
        self.last_log_btn.setObjectName('log_btn')
        self.last_log_btn.setFixedWidth(40)
        self.last_log_btn.setFixedHeight(40)
        self.last_log_btn.clicked.connect(lambda: self.set_last_log_visible())

        # Поле с текстом последнего бэкапа
        self.last_log_textbox = QTextEdit(self)
        self.last_log_textbox.setReadOnly(True)
        self.last_log_textbox.setFixedHeight(0)
        self.last_log_textbox.setFixedWidth(740)

        # Макет и наполнение таблицы снаружи журнала изменений
        self.bottom_grid = QGridLayout()
        self.bottom_grid.setContentsMargins(10, 0, 10, 0)
        self.bottom_grid.addWidget(self.change_log, 0, 0, 1, 3)
        self.bottom_grid.addWidget(self.last_launch, 1, 0)
        self.bottom_grid.addWidget(self.last_log_btn, 1, 0, 1, 3, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.bottom_grid.addWidget(self.latest_backups_info, 1, 1, alignment=Qt.AlignmentFlag.AlignLeft)
        self.bottom_grid.addWidget(self.manual_strt_btn, 1, 2, alignment=Qt.AlignmentFlag.AlignRight)
        self.bottom_grid.addWidget(self.last_log_textbox, 2, 0, 1, 3, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.bottom_grid.setRowMinimumHeight(1, 40)
        self.bottom_grid.setGeometry(QRect(5, 290, 760, 255))

        # Онлайн-время
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.showtime)
        self.timer.setInterval(1000)
        self.timer.start()

        # Лейбл часов
        self.label_datetime = QLabel(self)
        self.label_datetime.setFont(QFont("consolas", 10))

        # Cтатусбар
        self.setStatusBar(QStatusBar(self))

        # Изменение статуса CRON'a
        self.chng_cron = QLabel(self)
        self.chng_cron.setFont(QFont("consolas", 10))
        self.statusBar().addWidget(self.chng_cron)

        if self.json_adder()['cron']:
            self.chng_cron.setText('¦ CRON: включён')
        else:
            self.chng_cron.setText('¦ CRON: выключен')
        self.statusBar().setFont(QFont("consolas", 10))
        self.statusBar().addPermanentWidget(self.showip())
        self.statusBar().addPermanentWidget(self.label_datetime)
        self.tab_widget()
        self.show()

    def set_last_log_visible(self):
        # Функция разворачивания окна с текстом последнего бэкапа
        if self.last_log_textbox.height() == 0:
            self.last_log_textbox.clear()
            self.last_log_btn.setIcon(QIcon(resource_path('assets/arrow-u.png')))

            # Текст лога из файла
            last_log_file = open(SUROK_Admin().json_adder()['path_to_backups'] + "/logs/" + SUROK_Admin().json_adder()['latest_run'] + ".log", 'r')
            last_log_text = last_log_file.readlines()
            last_log_file.close()

            # Формат для окрашивания разного рода ошибок
            black = '<span style="color:Black;">'
            yellow = '<span style="color:Orange;">'
            red = '<span style="color:OrangeRed;">'
            bold_red = '<span style="color:Crimson;">'
            reset = '</span>'

            for line in last_log_text:
                if "DEBUG" in line:
                    self.last_log_textbox.append(f'{black}{line}{reset}')
                elif "INFO" in line:
                    self.last_log_textbox.append(f'{black}{line}{reset}')
                elif "WARNING" in line:
                    self.last_log_textbox.append(f'{yellow}{line}{reset}')
                elif "ERROR" in line:
                    self.last_log_textbox.append(f'{red}{line}{reset}')
                elif "CRITICAL" in line:
                    self.last_log_textbox.append(f'{bold_red}{line}{reset}')
                else:
                    self.last_log_textbox.append(line)

            self.setFixedSize(QSize(770, 776))
            self.last_log_textbox.setFixedHeight(200)
        else:
            self.last_log_btn.setIcon(QIcon(resource_path('assets/arrow-d.png')))
            self.last_log_textbox.setFixedHeight(0)
            self.setFixedSize(QSize(770, 576))

    def open_bckp_selection(self):
        # Отображение окна выбора бэкапа
        self.b_sel.show()

    def open_latest_backups_info(self):
        # Отображение окна информации и последнем бэкапе
        Last_backup_info().show()
        QApplication.processEvents()
        Last_backup_info().add_last_backups_data()

    def set_alerts(self):
        # Установка или снятие уведослений по почте
        if self.json_adder()['mail_alerts']:
            self.label_alerts.setIcon(QIcon(resource_path('assets/alert-off.png')))
            self.json_quiet_adder('mail_alerts', False)
        else:
            self.label_alerts.setIcon(QIcon(resource_path('assets/alert-on.png')))
            self.json_quiet_adder('mail_alerts', True)

    @staticmethod
    def open_diagnostics():
        # Отображение окна диагностики
        Diagnostics().show()
        QApplication.processEvents()
        Diagnostics().nw_con()
        QApplication.processEvents()
        Diagnostics().db_con()
        QApplication.processEvents()
        Diagnostics().fs_con()
        QApplication.processEvents()
        Diagnostics().ml_con()
        QApplication.processEvents()

    def showtime(self):
        # Онлайн-часы
        try:
            self.datetime = QDateTime.currentDateTime()
            self.text = self.datetime.toString()
            self.label_datetime.setText("   " + self.text)
        except KeyboardInterrupt:
            pass

    def showip(self):
        # Определение публичного ip-адреса
        self.ip_data = QLabel()
        self.ip_data.setFont(QFont("consolas", 10))
        try:
            # !!! В случае прекращения работы API, изменить ссылку !!!
            self.ip_data.setText("Your IP: " + get('https://api.ipify.org').content.decode('utf8'))
        except Exception:
            self.ip_data.setText("ERROR IP API CON.")
        return self.ip_data

    def open_file(self, path_dir):
        # Открытие папки в терминале, учитывая ОС
        if platform.system() == "Windows":
            os.startfile(path_dir)
        elif platform.system() == "Darwin":
            subprocess.Popen(["open", path_dir])
        else:
            subprocess.Popen(["xdg-open", path_dir])

    @staticmethod
    def json_quiet_adder(element, value):
        # "Тихая" версия функции ниже, изменяет JSON-файл настроек, без вмешательства в историю изменений
        json_file = open(resource_path("settings.json"), "r+", encoding='utf8')
        db_data = json.load(json_file)
        db_data[element] = value
        json_file.seek(0)
        json.dump(db_data, json_file, ensure_ascii=False, indent=4)
        json_file.truncate()
        json_file.close()

    def json_adder(self, element=None, value=None, multi=False):
        # Многоцелевая ф-ия, которая читает json-файл настроек, добавляет в него значения, возвращает его, и т.п.
        json_file = open(resource_path("settings.json"), "r+", encoding='utf8')
        db_data = json.load(json_file)
        if element and value and not multi:
            db_data["asked_value"] = value
            if value != db_data[element]:
                self.old_json_value = db_data[element]
                db_data[element] = value
                json_file.seek(0)
                json.dump(db_data, json_file, ensure_ascii=False, indent=4)
                json_file.truncate()
                self.new_json_value = value
                self.message_list.append(dict(old=self.old_json_value, new=self.new_json_value, el=element))
                json_file.close()
            else:
                db_data["asked_value"] = "STOP_ROLLBACK"
                json_file.seek(0)
                json.dump(db_data, json_file, ensure_ascii=False, indent=4)
                json_file.truncate()
                self.new_json_value = value
                json_file.close()
        elif element and value and multi:
            if not all(item in db_data[element] for item in value.replace(' ', '').split(",")):
                db_data["asked_value"] = value
                self.old_json_value = db_data[element].copy()
                list_value = value.replace(' ', '').split(",")
                db_data[element].extend(list_value)
                db_data[element] = list(dict.fromkeys(db_data[element]))
                json_file.seek(0)
                json.dump(db_data, json_file, ensure_ascii=False, indent=4)
                json_file.truncate()
                self.new_json_value = db_data[element]
                self.message_list.append(dict(old=self.old_json_value, new=self.new_json_value, el=element))
                json_file.close()
            else:
                db_data["asked_value"] = "STOP_ROLLBACK"
                json_file.seek(0)
                json.dump(db_data, json_file, ensure_ascii=False, indent=4)
                json_file.truncate()
                self.new_json_value = value
                json_file.close()
        else:
            json_file.close()
        return db_data

    def json_remover(self, element, value, multi=False):
        # Удаление значений в json-файле настроек
        json_file = open(resource_path("settings.json"), "r+", encoding='utf8')
        db_data = json.load(json_file)
        self.old_json_value = db_data[element].copy()
        if value != self.json_adder()[element]:
            if not multi:
                try:
                    db_data["asked_value"] = value
                    db_data[element].remove(value)
                except ValueError:
                    pass
            else:
                db_data["asked_value"] = value
                db_data[element] = value
            json_file.seek(0)
            json.dump(db_data, json_file, ensure_ascii=False, indent=4)
            json_file.truncate()
            self.new_json_value = db_data[element]
            self.message_list.append(dict(old=self.old_json_value, new=self.new_json_value, el=element))
            json_file.close()
        else:
            db_data["asked_value"] = "STOP_ROLLBACK"
            json_file.seek(0)
            json.dump(db_data, json_file, ensure_ascii=False, indent=4)
            json_file.truncate()
            self.new_json_value = db_data[element]
            json_file.close()

    def db_getter(self):
        # Создаём соединение с БД
        try:
            connection = MySQLdb.connect(
                host=self.json_adder()['host'],
                user=self.json_adder()['login'],
                password=self.json_adder()['password'],
                db=self.json_adder()['fs_db'],
                ssl={'key': self.json_adder()["path_to_ckc"], 'ca': self.json_adder()["path_to_scc"], 'cert': self.json_adder()["path_to_ccc"]}
            )
            cursor = connection.cursor()
        except Exception:
            return ('Ошибка! Запустите диагностику!',)

        # Создаём кортеж данных из пары значений (ID проекта, Имя проекта), если проект имеет флаг "L", он бэкапится (знак 1) и является Базой Данных
        try:
            cursor.execute('''
                SELECT Project_Name
                FROM projects
                WHERE Project_Status = 'L' AND Project_Backup = '1' AND Project_Project_Type_ID = '1'
                ''')
            all_db_tuple = cursor.fetchall()
            cursor.execute('''
                SELECT Project_ID, Project_Name
                FROM projects
                WHERE Project_Status = 'L' AND Project_Backup = '1' AND Project_Project_Type_ID = '1'
                ''')
            all_combo_tuple = cursor.fetchall()
            connection.close()
            all_db_tuple_res = []
            all_combo_dict_res = {}
            for db in all_db_tuple:
                all_db_tuple_res.append(db[0])
            for combo in all_combo_tuple:
                all_combo_dict_res[combo[1]] = combo[0]
            return tuple(all_db_tuple_res), all_combo_dict_res
        except Exception as e:
            return ('Ошибка! Запустите диагностику!', e)

    def pem_dialog(self, element):
        # Выбор pem-файла ssl-сертификата
        file, check = QFileDialog.getOpenFileName(None, "QFileDialog.getOpenFileName()",
                                                  "", "PEM Files (*.pem)")
        if check:
            self.json_adder(element=element, value=file)
            self.roll_back_msg(felement=element)

    def folder_dialog(self, element):
        # Выбор папки сохранения бэкапов
        folder = QFileDialog.getExistingDirectory(self, 'Select Folder')
        if folder:
            self.json_adder(element=element, value=folder)
            self.roll_back_msg(felement=element)

    # def update_db_list(self, db_list):
    #     # Обновление списка названий бд для бэкапа в интерфейсе
    #     db_list.clear()
    #     for num, db in enumerate(self.json_adder()["list_of_db"]):
    #         db_list.addItem("")
    #         db_list.setItemText(num, db)
    #     return db_list

    def update_db_list_new(self, db_list):
        # Обновление списка названий бд для бэкапа в интерфейсе
        db_list.clear()
        for num, db in enumerate(self.db_getter()[0]):
            db_list.addItem("")
            db_list.setItemText(num, db)
        return db_list

    def update_edit_line(self):
        # Обновление значений всех текстовых строк в интерфейсе
        # 1-ый таб
        self.update_db_list_new(self.all_db_combo)
        self.fs_db_edit.setText(self.json_adder()["fs_db"])
        self.bckp_path_label.setText(self.json_adder()["path_to_backups"])
        self.limit_save_edit.setValue(int(self.json_adder()["backup_age"]))
        # 2-ой таб
        self.db_host_edit.setText(self.json_adder()["host"])
        self.db_login_edit.setText(self.json_adder()["login"])
        self.db_pass_edit.setText(self.json_adder()["password"])
        self.ckc_path_label.setText(self.json_adder()["path_to_ckc"])
        self.ccc_path_label.setText(self.json_adder()["path_to_ccc"])
        self.scc_path_label.setText(self.json_adder()["path_to_scc"])
        # 3-ий таб
        self.mail_email_edit.setText(self.json_adder()["mail_email"])
        self.mail_id_edit.setText(self.json_adder()["mail_user_id"])
        self.mail_secret_edit.setText(self.json_adder()["mail_user_secret"])
        self.rprts_email_edit.setText(self.json_adder()["report_email"])
        # 4-ый таб
        self.cron_hours_combo.setCurrentText(self.json_adder()["cron_hour"])
        self.cron_minutes_combo.setCurrentText(self.json_adder()["cron_minute"])
        self.cron_date_combo.setCurrentText(self.json_adder()['cron_mode'])
        self.cron_day_combo.setCurrentText(self.json_adder()['cron_dow'])
        # Низ страницы
        self.last_launch.setText(f"Последний запуск: {self.json_adder()['latest_run']}")

    def roll_back_msg(self, rollback=False, roll_new="", felement="", multi=False, delete=False, not_the_last_change_for_one_button=False):
        # Многоцелевая ф-ия, которая обеспечивает корректную отмену (откат, возврат) значения по нажатию кнопки в интерфейсе
        if str(self.json_adder()["asked_value"]) != "STOP_ROLLBACK" and self.old_json_value != None and self.new_json_value != None:
            time_msg = QTime.currentTime().toString()
            fvalue = self.json_adder()
            fold = fvalue[felement]
            if not rollback and not multi:
                data_msg = f'Значение {"·" * len(self.old_json_value) if "password" in felement or "secret" in felement else self.old_json_value} изменено на {"·" * len(self.new_json_value) if "password" in felement or "secret" in felement else self.new_json_value}'
            elif rollback and not multi:
                data_msg = f'Значение {"·" * len(self.old_json_value) if "password" in felement or "secret" in felement else self.old_json_value} возвращено к {"·" * len(roll_new) if "password" in felement or "secret" in felement else roll_new}'
                if 'cron' in felement:
                    self.tmblr_cron.setChecked(False)
            elif not rollback and multi:
                fadded = [item for item in self.json_adder()["asked_value"].replace(' ', '').split(",") if
                          item not in self.old_json_value]
                data_msg = f'К значению "{self.old_json_value}" добавлено "{fadded}"'
            else:
                if not delete:
                    data_msg = f'Значение {self.old_json_value} возвращено к {roll_new}'
                else:
                    data_msg = f'Из значения {self.old_json_value} удалено "{self.json_adder()["asked_value"]}"'
            if not multi:
                if str(self.json_adder()["asked_value"]) == str(fold):
                    if self.layout_log.rowCount() > 5:
                        to_delete1 = self.layout_log.itemAtPosition(self.layout_log.rowCount() - 5, 0).widget()
                        to_delete2 = self.layout_log.itemAtPosition(self.layout_log.rowCount() - 5, 1).widget()
                        to_delete3 = self.layout_log.itemAtPosition(self.layout_log.rowCount() - 5, 2).widget()
                        to_delete1.deleteLater()
                        to_delete2.deleteLater()
                        to_delete3.deleteLater()
                    else:
                        pass
                    self.layout_log.addWidget(QLabel(time_msg), self.layout_log.rowCount(), 0,
                                              alignment=Qt.AlignmentFlag.AlignLeft)
                    self.layout_log.addWidget(QLabel(data_msg), self.layout_log.rowCount() - 1, 1,
                                              alignment=Qt.AlignmentFlag.AlignLeft)
                    undo_button = QPushButton(f"Отменить {self.layout_log.rowCount() - 1}")
                    undo_button.setFixedSize(90, 35)
                    undo_button.setObjectName("undo_btn")
                    undo_button.clicked.connect(lambda: self.json_adder(
                        element=self.message_list[int(undo_button.text().split(' ')[1]) - 1]["el"],
                        value=self.message_list[int(undo_button.text().split(' ')[1]) - 1]["old"]))
                    undo_button.clicked.connect(lambda: self.roll_back_msg(rollback=True, roll_new=
                    self.message_list[int(undo_button.text().split(' ')[1]) - 1]["old"], felement=self.message_list[
                        int(undo_button.text().split(' ')[1]) - 1]["el"]))
                    # undo_button.clicked.connect(lambda: self.update_db_list(self.all_db_combo))
                    self.layout_log.addWidget(undo_button, self.layout_log.rowCount() - 1, 2,
                                              alignment=Qt.AlignmentFlag.AlignRight)
                if not not_the_last_change_for_one_button:
                    self.update_edit_line()
                else:
                    pass
            else:
                if self.layout_log.rowCount() > 5:
                    to_delete1 = self.layout_log.itemAtPosition(self.layout_log.rowCount() - 5, 0).widget()
                    to_delete2 = self.layout_log.itemAtPosition(self.layout_log.rowCount() - 5, 1).widget()
                    to_delete3 = self.layout_log.itemAtPosition(self.layout_log.rowCount() - 5, 2).widget()
                    to_delete1.deleteLater()
                    to_delete2.deleteLater()
                    to_delete3.deleteLater()
                else:
                    pass
                if not all(item in fold for item in self.json_adder()["asked_value"]) or str(
                        self.message_list[-1]["old"] == []):
                    self.layout_log.addWidget(QLabel(time_msg), self.layout_log.rowCount(), 0,
                                              alignment=Qt.AlignmentFlag.AlignLeft)
                    self.layout_log.addWidget(QLabel(data_msg), self.layout_log.rowCount() - 1, 1,
                                              alignment=Qt.AlignmentFlag.AlignLeft)
                    undo_button = QPushButton(f"Отменить {self.layout_log.rowCount() - 1}")
                    undo_button.setFixedSize(90, 35)
                    undo_button.setObjectName("undo_btn")
                    undo_button.clicked.connect(lambda: self.json_remover(
                        element=self.message_list[int(undo_button.text().split(' ')[1]) - 1]["el"],
                        value=self.message_list[int(undo_button.text().split(' ')[1]) - 1]["old"], multi=True))
                    undo_button.clicked.connect(lambda: self.roll_back_msg(rollback=True, roll_new=
                    self.message_list[int(undo_button.text().split(' ')[1]) - 1]["old"], felement=self.message_list[
                        int(undo_button.text().split(' ')[1]) - 1]["el"], multi=True))
                    # undo_button.clicked.connect(lambda: self.update_db_list(self.all_db_combo))
                    self.layout_log.addWidget(undo_button, self.layout_log.rowCount() - 1, 2,
                                              alignment=Qt.AlignmentFlag.AlignRight)
                if not not_the_last_change_for_one_button:
                    self.update_edit_line()
                else:
                    pass
        else:
            pass

    def tab_widget(self):
        # Создание табов вверху окна

        # Создаём табы
        self.group1 = QWidget()
        self.group2 = QWidget()
        self.group3 = QWidget()
        self.group4 = QWidget()

        # Виджет таба
        self.tab_wdgt = QTabWidget(self)
        self.tab_wdgt.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Ignored)
        self.tab_wdgt.move(15, 55)
        self.tab_wdgt.setFixedSize(QSize(740, 225))

        ### --- BACKUP ПЕРВЫЙ ТАБ --- ###

        # Выпадающий список названий бд
        self.all_db_combo = QComboBox(self)
        self.all_db_combo.setFixedHeight(22)
        # self.update_db_list(self.all_db_combo) # Старый вызов, где данные берутся с JSON-файла
        self.update_db_list_new(self.all_db_combo)

        # # Строка ввода названия бд для бэкапа бд, для добавления в список
        # self.db_add_edit = QLineEdit(self)
        # self.db_add_edit.setPlaceholderText("dbname ИЛИ dbname1, dbname2, dbname3...")

        # Строка ввода названия бд для бэкапа фс
        self.fs_db_edit = QLineEdit(self)
        self.fs_db_edit.setText(self.json_adder()["fs_db"])

        # Лейбл пути сохранения бэкапов
        self.bckp_path_label = QLabel(self)
        self.bckp_path_label.setText(self.json_adder()["path_to_backups"])

        # Строка ввода ограничения по автоудалению старых бэкапов (дн.)
        self.limit_save_edit = QSpinBox(self)
        self.limit_save_edit.setRange(0, 9999)
        self.limit_save_edit.setValue(int(self.json_adder()["backup_age"]))

        # # Кнопка удаления названия бд для бэкапа бд
        # self.db_del_btn = QPushButton(' Удалить ')
        # self.db_del_btn.setObjectName("list_of_db")
        # self.db_del_btn.clicked.connect(lambda: self.json_remover(element=self.db_del_btn.objectName(), value=str(self.all_db_combo.currentText())))
        # self.db_del_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.db_del_btn.objectName(), rollback=True, multi=True, delete=True))
        # self.db_del_btn.clicked.connect(lambda: self.update_db_list(self.all_db_combo))
        # self.db_del_btn.setFixedWidth(92)

        # # Кнопка добавления названия(-ий) бд для бэкапа бд
        # self.db_add_btn = QPushButton('Добавить')
        # self.db_add_btn.setObjectName("list_of_db")
        # self.db_add_btn.clicked.connect(lambda: self.json_adder(element=self.db_add_btn.objectName(), value=self.db_add_edit.text(), multi=True))
        # self.db_add_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.db_add_btn.objectName(), multi=True))
        # self.db_add_btn.clicked.connect(lambda: self.update_db_list(self.all_db_combo))
        # self.db_add_btn.setFixedWidth(92)

        # Кнопка обновления названия(-ий) бд для бэкапа бд
        self.db_ref_btn = QPushButton('Обновить')
        self.db_ref_btn.clicked.connect(lambda: self.update_db_list_new(self.all_db_combo))
        self.db_ref_btn.setFixedWidth(92)

        # Кнопка изменения названия бд для бэкапа фс
        self.fs_db_btn = QPushButton('Изменить')
        self.fs_db_btn.setObjectName("fs_db")
        self.fs_db_btn.clicked.connect(lambda: self.json_adder(element=self.fs_db_btn.objectName(), value=self.fs_db_edit.text()))
        self.fs_db_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.fs_db_btn.objectName()))
        self.fs_db_btn.setFixedWidth(92)

        # Меню выбора открытия папки с бэкапами или логами
        self.open_menu = QMenu(self)
        self.open_menu.addAction("Бэкапы", lambda: self.open_file(self.bckp_path_label.text()))
        self.open_menu.addAction("Логи", lambda: self.open_file(f'{self.bckp_path_label.text()}/logs'))

        # Кнопка открытия папки с сохранёнными бэкапами или логами
        self.bckp_open_btn = QPushButton('Откр.')
        self.bckp_open_btn.setObjectName("open_bckp")
        self.bckp_open_btn.setMenu(self.open_menu)
        self.bckp_open_btn.setFixedWidth(74)

        # Кнопка выбора папки для сохранения бэкапов
        self.bckp_path_btn = QPushButton('Выбрать')
        self.bckp_path_btn.setObjectName("path_to_backups")
        self.bckp_path_btn.clicked.connect(lambda: self.folder_dialog(element=self.bckp_path_btn.objectName()))
        self.bckp_path_btn.clicked.connect(lambda: self.json_adder(element=self.bckp_path_btn.objectName(), value=self.bckp_path_label.text()))
        self.bckp_path_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.bckp_path_btn.objectName()))
        self.bckp_path_btn.setFixedWidth(92)

        # Кнопка изменения ограничения по автоудалению старых бэкапов (дн.)
        self.limit_save_btn = QPushButton('Изменить')
        self.limit_save_btn.setObjectName("backup_age")
        self.limit_save_btn.clicked.connect(lambda: self.json_adder(element=self.limit_save_btn.objectName(), value=str(self.limit_save_edit.text())))
        self.limit_save_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.limit_save_btn.objectName()))
        self.limit_save_btn.setFixedWidth(92)

        ### --- CONNECT ВТОРОЙ ТАБ --- ###

        # Строка ввода хоста
        self.db_host_edit = QLineEdit(self)
        self.db_host_edit.setText(self.json_adder()["host"])

        # Строка ввода логина
        self.db_login_edit = QLineEdit(self)
        self.db_login_edit.setText(self.json_adder()["login"])

        # Строка ввода пароля
        self.db_pass_edit = QLineEdit(self)
        self.db_pass_edit.setText(self.json_adder()["password"])
        self.db_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)

        # Лейбл пути файла ssl-сертификата (key)
        self.ckc_path_label = QLabel(self)
        self.ckc_path_label.setText(self.json_adder()["path_to_ckc"])

        # Лейбл пути файла ssl-сертификата (cert)
        self.ccc_path_label = QLabel(self)
        self.ccc_path_label.setText(self.json_adder()["path_to_ccc"])

        # Лейбл пути файла ssl-сертификата (serv. ca)
        self.scc_path_label = QLabel(self)
        self.scc_path_label.setText(self.json_adder()["path_to_scc"])

        # Кнопка изменения хоста
        self.db_host_btn = QPushButton('Изменить')
        self.db_host_btn.setObjectName("host")
        self.db_host_btn.clicked.connect(lambda: self.json_adder(element=self.db_host_btn.objectName(), value=self.db_host_edit.text()))
        self.db_host_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.db_host_btn.objectName()))
        self.db_host_btn.setFixedWidth(92)

        # Кнопка изменения логина
        self.db_login_btn = QPushButton('Изменить')
        self.db_login_btn.setObjectName("login")
        self.db_login_btn.clicked.connect(lambda: self.json_adder(element=self.db_login_btn.objectName(), value=self.db_login_edit.text()))
        self.db_login_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.db_login_btn.objectName()))
        self.db_login_btn.setFixedWidth(92)

        # Кнопка изменения пароля
        self.db_pass_btn = QPushButton('Изменить')
        self.db_pass_btn.setObjectName("password")
        self.db_pass_btn.clicked.connect(lambda: self.json_adder(element=self.db_pass_btn.objectName(), value=self.db_pass_edit.text()))
        self.db_pass_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.db_pass_btn.objectName()))
        self.db_pass_btn.setFixedWidth(92)

        # Кнопка выбора pem-файла ssl-сертификата (key)
        self.ckc_path_btn = QPushButton('Выбрать')
        self.ckc_path_btn.setObjectName("path_to_ckc")
        self.ckc_path_btn.clicked.connect(lambda: self.pem_dialog(element=self.ckc_path_btn.objectName()))
        self.ckc_path_btn.clicked.connect(lambda: self.json_adder(element=self.ckc_path_btn.objectName(), value=self.ckc_path_label.text()))
        self.ckc_path_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.ckc_path_btn.objectName()))
        self.ckc_path_btn.setFixedWidth(92)

        # Кнопка выбора pem-файла ssl-сертификата (cert)
        self.ccc_path_btn = QPushButton('Выбрать')
        self.ccc_path_btn.setObjectName("path_to_ccc")
        self.ccc_path_btn.clicked.connect(lambda: self.pem_dialog(element=self.ccc_path_btn.objectName()))
        self.ccc_path_btn.clicked.connect(lambda: self.json_adder(element=self.ccc_path_btn.objectName(), value=self.ccc_path_label.text()))
        self.ccc_path_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.ccc_path_btn.objectName()))
        self.ccc_path_btn.setFixedWidth(92)

        # Кнопка выбора pem-файла ssl-сертификата (serv. ca)
        self.scc_path_btn = QPushButton('Выбрать')
        self.scc_path_btn.setObjectName("path_to_scc")
        self.scc_path_btn.clicked.connect(lambda: self.pem_dialog(element=self.scc_path_btn.objectName()))
        self.scc_path_btn.clicked.connect(lambda: self.json_adder(element=self.scc_path_btn.objectName(), value=self.scc_path_label.text()))
        self.scc_path_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.scc_path_btn.objectName()))
        self.scc_path_btn.setFixedWidth(92)

        ### --- SMTP ТРЕТИЙ ТАБ --- ###

        # Строка ввода email'a приложения
        self.mail_email_edit = QLineEdit(self)
        self.mail_email_edit.setText(self.json_adder()["mail_email"])

        # Строка ввода ID пользователя
        self.mail_id_edit = QLineEdit(self)
        self.mail_id_edit.setText(self.json_adder()["mail_user_id"])

        # Строка ввода Secret'a пользователя
        self.mail_secret_edit = QLineEdit(self)
        self.mail_secret_edit.setText(self.json_adder()["mail_user_secret"])
        self.mail_secret_edit.setEchoMode(QLineEdit.EchoMode.Password)

        # Строка ввода email-адреса для получения лог-отчётов
        self.rprts_email_edit = QLineEdit(self)
        self.rprts_email_edit.setText(self.json_adder()["report_email"])
        self.rprts_email_edit.setFixedWidth(246)

        # Кнопка изменения email'a приложения
        self.mail_email_btn = QPushButton('Изменить')
        self.mail_email_btn.setObjectName("mail_email")
        self.mail_email_btn.clicked.connect(lambda: self.json_adder(element=self.mail_email_btn.objectName(), value=self.mail_email_edit.text()))
        self.mail_email_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.mail_email_btn.objectName()))
        self.mail_email_btn.setFixedWidth(92)

        # Кнопка изменения ID приложения
        self.mail_id_btn = QPushButton('Изменить')
        self.mail_id_btn.setObjectName("mail_user_id")
        self.mail_id_btn.clicked.connect(lambda: self.json_adder(element=self.mail_id_btn.objectName(), value=self.mail_id_edit.text()))
        self.mail_id_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.mail_id_btn.objectName()))
        self.mail_id_btn.setFixedWidth(92)

        # Кнопка изменения Secret'a пользователя
        self.mail_secret_btn = QPushButton('Изменить')
        self.mail_secret_btn.setObjectName("mail_user_secret")
        self.mail_secret_btn.clicked.connect(lambda: self.json_adder(element=self.mail_secret_btn.objectName(), value=self.mail_secret_edit.text()))
        self.mail_secret_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.mail_secret_btn.objectName()))
        self.mail_secret_btn.setFixedWidth(92)

        # Сообщение об отправке тестового письма
        self.messageM = QMessageBox()
        self.messageM.setIcon(QMessageBox.Icon.Information)
        self.messageM.setInformativeText("Тестовое письмо отправлено!")

        # Кнопка отправки тестового сообщения на указанный email-адрес
        self.rprts_test_btn = QPushButton('Тест')
        self.rprts_test_btn.setObjectName("report_test")
        self.rprts_test_btn.clicked.connect(lambda: self.send_test_letter())
        self.rprts_test_btn.setFixedWidth(74)

        # Кнопка изменения email-адреса для отправки лог-отчётов
        self.rprts_email_btn = QPushButton('Изменить')
        self.rprts_email_btn.setObjectName("report_email")
        self.rprts_email_btn.clicked.connect(lambda: self.json_adder(element=self.rprts_email_btn.objectName(), value=self.rprts_email_edit.text()))
        self.rprts_email_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.rprts_email_btn.objectName()))
        self.rprts_email_btn.setFixedWidth(92)

        ### --- CRON ЧЕТВËРТЫЙ ТАБ --- ###

        # Выпадающий список CRON: день недели (опционный)
        self.cron_day_combo = QComboBox(self)
        self.cron_day_combo.setObjectName('cron_dow')
        self.cron_day_combo.addItems(["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"])
        self.cron_day_combo.setFixedWidth(130)
        self.cron_day_combo.setFixedHeight(22)
        self.cron_day_combo.setCurrentText(self.json_adder()['cron_dow'])
        if self.json_adder()['cron_mode'] == 'Еженедельно':
            self.cron_day_combo.setDisabled(False)
        else:
            self.cron_day_combo.setDisabled(True)

        # Выпадающий список CRON: неделя-день
        self.cron_date_combo = QComboBox(self)
        self.cron_date_combo.setObjectName('cron_mode')
        self.cron_date_combo.addItems(["Ежедневно", "Еженедельно"])
        self.cron_date_combo.setFixedWidth(130)
        self.cron_date_combo.setFixedHeight(22)
        self.cron_date_combo.currentIndexChanged.connect(self.cron_period_change)
        self.cron_date_combo.setCurrentText(self.json_adder()['cron_mode'])

        # Выпадающий список CRON: час
        self.cron_hours_combo = QComboBox(self)
        self.cron_hours_combo.setObjectName('cron_hour')
        self.cron_hours_combo.setToolTip("Часы")
        self.cron_hours_array = [(time(i).strftime('%H')) for i in range(24)]
        self.cron_hours_combo.addItems(self.cron_hours_array)
        self.cron_hours_combo.setCurrentText(self.json_adder()['cron_hour'])
        self.cron_hours_combo.setFixedHeight(22)

        # Выпадающий список CRON: минута
        self.cron_minutes_combo = QComboBox(self)
        self.cron_minutes_combo.setObjectName('cron_minute')
        self.cron_minutes_combo.setToolTip("Минуты")
        self.cron_minutes_array = [datetime.strptime(str(i*timedelta(minutes=15)),'%H:%M:%S').strftime('%M') for i in range(60//15)]
        self.cron_minutes_combo.addItems(self.cron_minutes_array)
        self.cron_minutes_combo.setCurrentText(self.json_adder()['cron_minute'])
        self.cron_minutes_combo.setFixedHeight(22)

        # Кнопка изменения параметров CRON'а
        self.cron_edit_btn = QPushButton('Изменить')
        self.cron_edit_btn.setObjectName("cron_edit")
        self.cron_edit_btn.setFixedWidth(92)
        self.cron_edit_btn.clicked.connect(lambda: self.json_adder(element=self.cron_date_combo.objectName(), value=self.cron_date_combo.currentText()))
        self.cron_edit_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.cron_date_combo.objectName(), not_the_last_change_for_one_button=True))
        self.cron_edit_btn.clicked.connect(lambda: self.json_adder(element=self.cron_day_combo.objectName(), value=self.cron_day_combo.currentText()))
        self.cron_edit_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.cron_day_combo.objectName(), not_the_last_change_for_one_button=True))
        self.cron_edit_btn.clicked.connect(lambda: self.json_adder(element=self.cron_hours_combo.objectName(), value=self.cron_hours_combo.currentText()))
        self.cron_edit_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.cron_hours_combo.objectName(), not_the_last_change_for_one_button=True))
        self.cron_edit_btn.clicked.connect(lambda: self.json_adder(element=self.cron_minutes_combo.objectName(), value=self.cron_minutes_combo.currentText()))
        self.cron_edit_btn.clicked.connect(lambda: self.roll_back_msg(felement=self.cron_minutes_combo.objectName()))
        self.cron_edit_btn.clicked.connect(lambda: self.tmblr_cron.setChecked(False))
        self.cron_edit_btn.clicked.connect(lambda: self.cron_turn_new())

        # Чекбокс выбора бэкапа БД CRON
        self.db_cron_cb = QCheckBox("б.БД")
        self.db_cron_cb.setToolTip("Бэкап баз данных")
        self.db_cron_cb.setChecked(self.json_adder()['cron_db'])
        self.db_cron_cb.stateChanged.connect(lambda: self.json_quiet_adder('cron_db', self.db_cron_cb.isChecked()))
        self.db_cron_cb.stateChanged.connect(lambda: self.cron_pars_chck('db'))

        # Чекбокс выбора бэкапа ФС CRON
        self.fs_cron_cb = QCheckBox("б.ФС")
        self.fs_cron_cb.setToolTip("Бэкап файловой системы")
        self.fs_cron_cb.setChecked(self.json_adder()['cron_fs'])
        self.fs_cron_cb.stateChanged.connect(self.cron_fs_chck)
        self.fs_cron_cb.stateChanged.connect(lambda: self.cron_pars_chck('fs'))

        # Чекбокс вкл/выкл проверку актуальности бэкапа
        self.fs_cron_ac = QCheckBox("Лишь последняя версия б.ФС")
        self.fs_cron_ac.setToolTip("Собрать лишь последнюю версию бэкапов файловой системы")
        self.fs_cron_ac.setChecked(self.json_adder()['cron_act_check'])
        self.fs_cron_ac.setDisabled(not self.json_adder()['cron_fs'])
        self.fs_cron_ac.stateChanged.connect(lambda: self.json_quiet_adder('cron_act_check', self.fs_cron_ac.isChecked()))

        # Макет ячейки с двумя чекбоксами
        self.cron_cb_cell = QHBoxLayout()
        self.cron_cb_cell.addWidget(self.db_cron_cb)
        self.cron_cb_cell.addWidget(self.fs_cron_cb)
        self.cron_cb_cell.setContentsMargins(0, 0, 0, 0)
        self.cron_cb_cell.setSpacing(37)

        # Виджет ячейки с двумя чекбоксами
        self.cron_cb_wdgt = QWidget(self)
        self.cron_cb_wdgt.setLayout(self.cron_cb_cell)

        # Тумблер включения CRON'а
        self.tmblr_cron = AnimatedToggle(checked_color="#006959")
        self.tmblr_cron.bar_checked_brush = QBrush(QColor('#9cb8b3'))
        self.tmblr_cron.setObjectName('cron_toogle')
        self.tmblr_cron.setFixedSize(QSize(38, 25))
        self.tmblr_cron.setChecked(self.json_adder()['cron'])
        self.tmblr_cron.toggled.connect(lambda: self.cron_turn_new())

        # Макет ячейки с тумблером включения
        self.cron_tmblr_cell = QHBoxLayout()
        self.cron_tmblr_cell.addWidget(QLabel('⠀⠀Вкл/Выкл CRON:'))
        self.cron_tmblr_cell.addWidget(self.tmblr_cron)
        self.cron_tmblr_cell.setContentsMargins(0, 0, 0, 0)
        self.cron_tmblr_cell.setSpacing(10)

        # Виджет ячейки с тумблером включения
        self.cron_tmblr_wdgt = QWidget(self)
        self.cron_tmblr_wdgt.setLayout(self.cron_tmblr_cell)

        # Лейбл предыдущей работы крона
        self.cron_count_job_dateP = QLabel(self)
        self.cron_count_job_dateP.setText(self.update_cron_dates()[0])

        # Лейбл следующей работы крона
        self.cron_count_job_dateN = QLabel(self)
        self.cron_count_job_dateN.setText(self.update_cron_dates()[1])

        # Обратный отсчёт - часы
        self.downtimer = QTimer()
        self.downtimer.timeout.connect(self.countdown)
        self.downtimer.setInterval(1000)
        self.downtimer.start()

        # Лейбл обратного отсчёта до следующей работы крона
        self.cron_countdown_job = QLabel(self)
        self.cron_countdown_job_brother = QLabel(self) # брат-близнец для другого окна (окно с предупреждениями-уведомлениями перед началом бэкапа)
        self.cron_countdown_job_brother.setHidden(True)
        if not self.json_adder()['cron']:
            self.cron_countdown_job.setHidden(True)

        # Макет ячейки с датами запусков CRON'a
        self.cron_dates_cell = QHBoxLayout()
        self.cron_dates_cell.addWidget(QLabel('Предыдущий запуск:'))
        self.cron_dates_cell.addWidget(self.cron_count_job_dateP)
        self.cron_dates_cell.addWidget(QLabel('  ¦  '))
        self.cron_dates_cell.addWidget(QLabel('Следующий запуск:'))
        self.cron_dates_cell.addWidget(self.cron_count_job_dateN)
        self.cron_dates_cell.setContentsMargins(0, 0, 0, 0)
        self.cron_dates_cell.setSpacing(10)

        # Виджет ячейки с датами запусков CRON'a
        self.cron_dates_wdgt = QWidget(self)
        self.cron_dates_wdgt.setLayout(self.cron_dates_cell)

        ### *** МАКЕТЫ ТАБОВ *** ###

        # Макет и наполнение таблицы первого таба
        self.grid1 = QGridLayout(self.group1)
        self.grid1.setColumnMinimumWidth(0, 212)
        self.grid1.setColumnMinimumWidth(2, 103)
        self.grid1.setContentsMargins(0, 0, 0, 0)
        self.grid1.setSpacing(10)
        self.grid1.addWidget(QLabel("Список БД для backup'a БД:"), 0, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid1.addWidget(self.all_db_combo, 0, 1)
        self.grid1.addWidget(self.db_ref_btn, 0, 2)
        # self.grid1.addWidget(QLabel("Добавить БД для backup'a БД:"), 1, 0, alignment=Qt.AlignmentFlag.AlignRight)
        # self.grid1.addWidget(self.db_add_edit, 1, 1)
        # self.grid1.addWidget(self.db_add_btn, 1, 2)
        self.grid1.addWidget(QLabel("БД для backup'a ФС:"), 1, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid1.addWidget(self.fs_db_edit, 1, 1)
        self.grid1.addWidget(self.fs_db_btn, 1, 2)
        self.grid1.addWidget(QLabel("Путь сохранения backup'ов:"), 2, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid1.addWidget(self.bckp_path_label, 2, 1)
        self.grid1.addWidget(self.bckp_open_btn, 2, 1, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid1.addWidget(self.bckp_path_btn, 2, 2)
        self.grid1.addWidget(QLabel("Удаление backup'ов на ПК (дн.):"), 3, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid1.addWidget(self.limit_save_edit, 3, 1)
        self.grid1.addWidget(self.limit_save_btn, 3, 2)

        # Макет и наполнение таблицы второго таба
        self.grid2 = QGridLayout(self.group2)
        self.grid2.setColumnMinimumWidth(0, 212)
        self.grid2.setColumnMinimumWidth(2, 172)
        self.grid2.setContentsMargins(0, 0, 0, 0)
        self.grid2.setSpacing(10)
        self.grid2.addWidget(QLabel("Сервер:"), 1, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid2.addWidget(self.db_host_edit, 1, 1)
        self.grid2.addWidget(self.db_host_btn, 1, 2)
        self.grid2.addWidget(QLabel("Логин:"), 2, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid2.addWidget(self.db_login_edit, 2, 1)
        self.grid2.addWidget(self.db_login_btn, 2, 2)
        self.grid2.addWidget(QLabel("Пароль:"), 3, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid2.addWidget(self.db_pass_edit, 3, 1)
        self.grid2.addWidget(self.db_pass_btn, 3, 2)
        self.grid2.addWidget(QLabel("Client key:"), 4, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid2.addWidget(self.ckc_path_label, 4, 1)
        self.grid2.addWidget(self.ckc_path_btn, 4, 2)
        self.grid2.addWidget(QLabel("Client cert.:"), 5, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid2.addWidget(self.ccc_path_label, 5, 1)
        self.grid2.addWidget(self.ccc_path_btn, 5, 2)
        self.grid2.addWidget(QLabel("Server CA:"), 6, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid2.addWidget(self.scc_path_label, 6, 1)
        self.grid2.addWidget(self.scc_path_btn, 6, 2)

        # Макет и наполнение таблицы третьего таба
        self.grid3 = QGridLayout(self.group3)
        self.grid3.setColumnMinimumWidth(0, 212)
        self.grid3.setColumnMinimumWidth(2, 172)
        self.grid3.setContentsMargins(0, 0, 0, 0)
        self.grid3.setSpacing(10)
        self.grid3.addWidget(QLabel('Email приложения:'), 0, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid3.addWidget(self.mail_email_edit, 0, 1)
        self.grid3.addWidget(self.mail_email_btn, 0, 2)
        self.grid3.addWidget(QLabel('ID пользователя:'), 1, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid3.addWidget(self.mail_id_edit, 1, 1)
        self.grid3.addWidget(self.mail_id_btn, 1, 2)
        self.grid3.addWidget(QLabel('Secret пользователя:'), 2, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid3.addWidget(self.mail_secret_edit, 2, 1)
        self.grid3.addWidget(self.mail_secret_btn, 2, 2)
        self.grid3.addWidget(QLabel("Email для отчётов:"), 3, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid3.addWidget(self.rprts_email_edit, 3, 1)
        self.grid3.addWidget(self.rprts_test_btn, 3, 1, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid3.addWidget(self.rprts_email_btn, 3, 2)

        # Макет и наполнение таблицы четвёртого таба
        self.grid4 = QGridLayout(self.group4)
        self.grid4.setColumnMinimumWidth(0, 236)
        self.grid4.setColumnMinimumWidth(1, 130)
        self.grid4.setColumnMinimumWidth(2, 60)
        self.grid4.setColumnMinimumWidth(3, 60)
        self.grid4.setContentsMargins(0, 0, 0, 0)
        self.grid4.setSpacing(10)
        self.grid4.addWidget(QLabel(''), 0, 0, 0, 7)
        self.grid4.addWidget(self.cron_date_combo, 1, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid4.addWidget(self.cron_day_combo, 1, 1)
        self.grid4.addWidget(self.cron_hours_combo, 1, 2)
        self.grid4.addWidget(self.cron_minutes_combo, 1, 3)
        self.grid4.addWidget(self.cron_edit_btn, 1, 4)
        self.grid4.addWidget(self.cron_cb_wdgt, 2, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.grid4.addWidget(self.fs_cron_ac, 2, 1, 1, 2)
        self.grid4.addWidget(self.cron_tmblr_wdgt, 2, 3, 1, 4, alignment=Qt.AlignmentFlag.AlignLeft)
        self.grid4.addWidget(self.cron_dates_wdgt, 3, 0, 3, 7, alignment=Qt.AlignmentFlag.AlignHCenter)
        self.grid4.addWidget(self.cron_countdown_job, 4, 0, 4, 7, alignment=Qt.AlignmentFlag.AlignHCenter)

        # Добавление табов
        self.tab_wdgt.addTab(self.group1, "Backup")
        self.tab_wdgt.addTab(self.group2, "MYSQL")
        self.tab_wdgt.addTab(self.group3, "SMTP")
        self.tab_wdgt.addTab(self.group4, "CRON")
        self.tab_wdgt.setTabToolTip(0, 'Данные для бэкапа на локальный ПК')
        self.tab_wdgt.setTabToolTip(1, 'Данные для подключения к БД MySQL для отсылки отчётов')
        self.tab_wdgt.setTabToolTip(2, 'Данные для подключения к SMTP-серверу для отсылки отчётов')
        self.tab_wdgt.setTabToolTip(3, 'Данные для установки CRON`a для планового бэкапа')

        # Окно уведомления о неправильной настройке почтовой отправки логов
        self.messageME = QMessageBox()
        self.messageME.setIcon(QMessageBox.Icon.Critical)
        self.messageME.setInformativeText("Ошибка отправки письма! Запустите диагностику и проверьте правильность написания адреса(ов)!")

        self.count_from_date = datetime.strptime(self.update_cron_dates()[-1], "%Y-%m-%d %H:%M:%S") - datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S") # Переменная класса вычтенного значения между планируемой датой-временем бэкапа и нынешней датой-временем

    def renew_subtracted_datetime(self):
        self.count_from_date = datetime.strptime(self.update_cron_dates()[-1], "%Y-%m-%d %H:%M:%S") - datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S") # Обновление переменной класса вычтенного значения между планируемой датой-временем бэкапа и нынешней датой-временем

    def countdown(self):
        # Каждую секунду отнимаем по 1 секунде от разницы нынешней даты и даты следующего бэкапа
        try:
            self.count_from_date = self.count_from_date - timedelta(seconds=1)
            self.cron_countdown_job.setText(str(self.count_from_date))
            self.cron_countdown_job_brother.setText(str(self.count_from_date))
            if "day" in str(self.count_from_date):
                self.cron_countdown_job_brother.setFixedWidth(105)
                self.cron_countdown_job.setFixedWidth(101)
            else:
                self.cron_countdown_job_brother.setFixedWidth(60)
                self.cron_countdown_job.setFixedWidth(56)
        except KeyboardInterrupt:
            pass

    def send_test_letter(self):
        # Отправка тестового письма
        try:
            mail.run(test=True)
            self.view = self.messageM.exec()
        except Exception:
            view = self.messageME.show()

    def cron_period_change(self, new_index):
        # Сделать неактивн. вып. список дней недели при определённом сигнале вып. списка выбора частоты бэкапа
        if new_index == 1:
            self.cron_day_combo.setDisabled(False)
        elif new_index == 0:
            self.cron_day_combo.setDisabled(True)

    def cron_fs_chck(self, new_state):
        # Сделать активн. опцию проверки актуальности бэкапа ФС при выборе данного бэкапа
        if new_state == 0:
            self.fs_cron_ac.setDisabled(True)
            self.fs_cron_ac.setChecked(False)
        else:
            self.fs_cron_ac.setDisabled(False)
        self.json_quiet_adder('cron_fs', self.fs_cron_cb.isChecked())

    def cron_pars_chck(self, cur_tmblr):
        # Выключение тумблера, если отключается последний чекбокс выбора бэкапа
        if cur_tmblr == 'db':
            state = self.fs_cron_cb.isChecked()
        else:
            state = self.db_cron_cb.isChecked()
        if not state:
            self.tmblr_cron.setChecked(False)

    def update_cron_dates(self):
        # Функция обновления значений времени запусков CRON (предыдущий - следующий)
        now = datetime.now()
        sched = self.json_adder()['cron_last_schedule']
        cron = croniter(sched, now)
        previousdate = cron.get_prev(datetime)
        cron_schedule_time = datetime.strptime(self.json_adder()['cron_schedule_time'], "%Y-%m-%d %H:%M:%S")
        nextdate = cron.get_next(datetime)

        if cron_schedule_time < previousdate:
            cron_dates_tuple = (str(previousdate), str(nextdate))
        else:
            cron_dates_tuple = ('never', str(nextdate))
        if self.json_adder()['cron']:
            return cron_dates_tuple
        else:
            return ('CRON не установлен', 'CRON не установлен', str(nextdate))

    def cron_turn_new(self):
        # Функция включения/выключения CRON'a
        if not self.json_adder()['cron_db'] and not self.json_adder()['cron_fs'] and self.tmblr_cron.isChecked():
            self.messageT = QMessageBox()
            self.messageT.setIcon(QMessageBox.Icon.Warning)
            self.messageT.setInformativeText("Выберите хотя-бы один бэкап!")
            self.view = self.messageT.exec()
            self.tmblr_cron.setCheckable(False)
            self.tmblr_cron.setCheckable(True)
        elif not self.json_adder()['cron_db'] and not self.json_adder()['cron_fs'] and not self.tmblr_cron.isChecked():
            self.json_quiet_adder('cron', False)
            surcron.remove_cron()
            self.chng_cron.setText('¦ CRON: выключен')
            self.cron_count_job_dateP.setText(self.update_cron_dates()[0])
            self.cron_count_job_dateN.setText(self.update_cron_dates()[1])
            self.cron_countdown_job.setHidden(True)
        else:
            if not self.tmblr_cron.isChecked():
                self.json_quiet_adder('cron', False)
                surcron.remove_cron()
                self.chng_cron.setText('¦ CRON: выключен')
                self.cron_count_job_dateP.setText(self.update_cron_dates()[0])
                self.cron_count_job_dateN.setText(self.update_cron_dates()[1])
                self.cron_countdown_job.setHidden(True)
            else:
                self.json_quiet_adder('cron', True)
                app_path = os.path.abspath(__file__)
                surcron.create_cron(path_to_app=app_path)
                self.chng_cron.setText('¦ CRON: включён')
                self.cron_count_job_dateP.setText(self.update_cron_dates()[0])
                self.cron_count_job_dateN.setText(self.update_cron_dates()[1])
                self.cron_countdown_job.setHidden(False)
                self.renew_subtracted_datetime()

class MinWorker(QThread):
    # Поток для работы CRON - бэкапа
    def __init__(self, func, args):
        super(MinWorker, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.func(*self.args)

class CRON_Widget(QWidget):
    # Класс интерфейса окна CRON-бэкапа
    def __init__(self):
        super().__init__()
        self.setWindowTitle('ПО "СУРОК"')
        self.setFixedSize(QSize(250, 56))

        # Установка окна в правом нижнем углу экрана
        self.bottom_right()

        # Макет окна - таблица
        self.cron_grid = QGridLayout()
        self.cron_grid.setSpacing(5)
        self.cron_grid.setContentsMargins(4, 0, 4, 0)

        # Установка макета
        self.setLayout(self.cron_grid)

        # Мини - лого
        self.label_logo_min = QLabel(self)
        self.pixmap_logo_min = QPixmap(resource_path("assets/logo-min.png")).scaled(46, 42, Qt.AspectRatioMode.KeepAspectRatio,
                                                             Qt.TransformationMode.SmoothTransformation)
        self.label_logo_min.setGeometry(QRect(0, 0, 38, 36))
        self.label_logo_min.setText("")
        self.label_logo_min.setPixmap(self.pixmap_logo_min)
        self.label_logo_min.setToolTip(f'"Система управления резервным объёмным копированием v.1.3.0 - © {date.today().year} IvNoch"')

        # Сообщение об активном плановом бэкапе
        self.label_after_run = QLabel(self)
        self.label_after_run.setText('Идёт плановый backup CRON')

        # Анимация трёх точек
        self.label_anim_gif = QMovie(resource_path("assets/dots.gif"))
        self.label_anim_gif.setScaledSize(QSize(10, 10))
        self.label_anim_label = QLabel(self)
        self.label_anim_label.setFixedSize(10, 10)
        self.label_anim_label.setMovie(self.label_anim_gif)
        self.label_anim_gif.start()

        # Добавление виджетов в макет
        self.cron_grid.addWidget(self.label_logo_min, 0, 0)
        self.cron_grid.addWidget(self.label_after_run, 0, 1)
        self.cron_grid.addWidget(self.label_anim_label, 0, 2)

        # Окно подтверждения остановки сценария бэкапа
        self.messageCS = QMessageBox()
        self.messageCS.setIcon(QMessageBox.Icon.Warning)
        self.messageCS.setInformativeText("Вы действительно хотите прервать сценарий бэкапа? Все файлы этой сессии будут удалены!")
        self.messageCS.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)

        # Окно уведомления об остановке сценария бэкапа и удалении файлов
        self.messageCI = QMessageBox()
        self.messageCI.setIcon(QMessageBox.Icon.Information)
        self.messageCI.setInformativeText("Сценарий прерван. Файлы удалены.")

        # Поток для работы бэкапа
        self.min_bee = MinWorker(self.cron_strt, ())
        self.min_bee.start()
        self.min_bee.finished.connect(self.cron_on_finish)

        # Настройки логгера и его хэндлера
        self.logger = logging.getLogger('logger')
        self.consoleHandler = ConsoleWindowLogHandler()
        self.consoleHandler.setFormatter(CustomFormatter())

    def bottom_right(self):
        # Позиционируем окно по правому нижнему углу экрана
        bottom_right_point = QApplication.primaryScreen().availableGeometry().bottomRight()
        self.move(bottom_right_point)

    def cron_on_finish(self):
        # Закрытие окна по окончании бэкапа
        sleep(4)
        self.close()

    def db_getter(self):
        # Создаём соединение с БД
        config_file = open(resource_path("settings.json"), 'r')
        config = json.load(config_file)  # json-файл конфигурации
        config_file.close()
        try:
            connection = MySQLdb.connect(
                host=config['host'],
                user=config['login'],
                password=config['password'],
                db=config['fs_db'],
                ssl={'key': config["path_to_ckc"], 'ca': config["path_to_scc"], 'cert': config["path_to_ccc"]}
            )
            cursor = connection.cursor()
        except Exception:
            sys.exit()

        # Создаём кортеж данных из пары значений (ID проекта, Имя проекта), если проект имеет флаг "L", он бэкапится (знак 1) и является Базой Данных
        try:
            cursor.execute('''
                SELECT Project_Name
                FROM projects
                WHERE Project_Status = 'L' AND Project_Backup = '1' AND Project_Project_Type_ID = '1'
                ''')
            all_db_tuple = cursor.fetchall()
            cursor.execute('''
                SELECT Project_ID, Project_Name
                FROM projects
                WHERE Project_Status = 'L' AND Project_Backup = '1' AND Project_Project_Type_ID = '1'
                ''')
            all_combo_tuple = cursor.fetchall()
            connection.close()
            all_db_tuple_res = []
            all_combo_dict_res = {}
            for db in all_db_tuple:
                all_db_tuple_res.append(db[0])
            for combo in all_combo_tuple:
                all_combo_dict_res[combo[1]] = combo[0]
            return tuple(all_db_tuple_res), all_combo_dict_res
        except Exception:
            sys.exit()

    def cron_strt(self):
        # CRON старт бэкапа
        try:
            run.cron_start(db_tuple=self.db_getter()[0], db_combo=self.db_getter()[1])
        except Exception:
            pass
        config_file = open(resource_path("settings.json"), 'r')
        config = json.load(config_file)  # json-файл конфигурации
        config_file.close()
        check_log_file = open(config['path_to_backups'] + "/logs/" + config['latest_run'] + ".log", 'r')
        check_log = check_log_file.read()
        check_log_file.close()
        if config['mail_alerts']:
            if 'WARNING' in check_log or 'ERROR' in check_log or 'CRITICAL' in check_log:
                try:
                    mail.run()  # Отсылаем отчёт письмом
                except Exception:
                    pass
        if dbbckp.EOFFlag or fsbckp.EOFFlag:
            try:
                self.delete_db_bckp_data(config)
            except Exception as e:
                self.logger.error(f'P.S. Ошибка удаления записи о бэкапах в БД {e}')
                self.logger.info('-------ПРЕРЫВАНИЕ УДАЛЕНИЯ ДАННЫХ О БЭКАПЕ С ОШИБКОЙ-------')
        self.label_anim_label.hide()
        self.label_after_run.setText('DONE')

    def delete_db_bckp_data(self, config):
        handlers = self.logger.handlers[:]
        for handler in handlers:
            self.logger.removeHandler(handler)
            handler.close()
        self.logger.addHandler(self.consoleHandler)
        fh = logging.FileHandler(
            f"{config['path_to_backups']}/logs/{config['latest_run']}.log",
            mode='a', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%d.%m.%Y %H:%M:%S'))
        self.logger.addHandler(fh)
        # 1. Удаляем данные о бэкапе из таблицы backup_tables, по полученным ID бэкапов, относительно времени последнего бэкапа
        # 2. Удаляем данные о бэкапе из таблицы backups, от имени второго бэкапера
        # Получаем форматированное значение даты и времени последнего бэкапа, в часовой зоне UTC
        q_d = datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M') - datetime.strptime(
            datetime.utcnow().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M')  # Сдвиг по UTC
        r_d = datetime.strptime(config["latest_run"], '%Y_%m_%d_%H_%M') - q_d
        formatted_cur_datetime_tuple = str(r_d.strftime('%Y_%m_%d_%H_%M')).split('_')
        formatted_cur_datetime = f'{formatted_cur_datetime_tuple[0]}-{formatted_cur_datetime_tuple[1]}-{formatted_cur_datetime_tuple[2]} {formatted_cur_datetime_tuple[3]}:{formatted_cur_datetime_tuple[4]}'

        connection = MySQLdb.connect(
            host=config['host'],
            user=config['login'],
            password=config['password'],
            db=config['fs_db'],
            ssl={'key': config["path_to_ckc"], 'ca': config["path_to_scc"], 'cert': config["path_to_ccc"]}
        )
        cursor = connection.cursor()
        # Получаем ID всех бэкапов, где время создания бэкапа совпадает с форматированным значением нашего последнего бэкапа
        cursor.execute('''SELECT Backup_ID
                        FROM backups
                        WHERE Backup_Create = %s''', [formatted_cur_datetime])
        cur_backups_ids = cursor.fetchall()
        formatted_cur_backups_ids = [x[0] for x in cur_backups_ids]

        for id in formatted_cur_backups_ids:
            # Удаляем записи о всех бэкапах, где ID  совпадают с полученными выше
            cursor.execute('''DELETE FROM backup_tables
                            WHERE Backup_Table_Backup_ID=%s;''', [id])
            connection.commit()
            cursor.execute('''DELETE FROM backup_files
                            WHERE Backup_File_Backup_ID=%s;''', [id])
            connection.commit()

        # Удаляем записи из таблицы backups, где время создания бэкапа совпадает с форматированным значением нашего последнего бэкапа
        cursor.execute('''DELETE FROM backups
                                WHERE Backup_Create=%s;''', [formatted_cur_datetime])
        connection.commit()
        connection.close()
        dbbckp.EOFFlag = False
        fsbckp.EOFFlag = False

    @staticmethod
    def delete_files_stp():
        # Удаление файлов прерванного бэкапа
        config_file = open(resource_path("settings.json"), 'r')
        config = json.load(config_file)  # json-файл конфигурации
        config_file.close()
        my_dir = config['path_to_backups']
        for fname in os.listdir(my_dir):
            if fname.startswith(config['latest_run']):
                try:
                    os.remove(os.path.join(my_dir, fname))
                except Exception:
                    shutil.rmtree(os.path.join(my_dir, fname))

    def closeEvent(self, event):
        # Сценарий закрытия окна
        if self.min_bee.isRunning():
            self.manual_stp()
            event.ignore()
        else:
            event.accept()

    def manual_stp(self):
        # Ручной останов бэкапа
        config_file = open(resource_path("settings.json"), 'r')
        config = json.load(config_file)  # json-файл конфигурации
        config_file.close()
        self.stp_que = self.messageCS.exec()
        if self.stp_que == QMessageBox.StandardButton.Yes:
            self.label_anim_label.hide()
            self.label_after_run.setText('ПРЕРЫВАНИЕ...')
            QApplication.processEvents()
            if config['cron_db']:
                dbbckp.EOFFlag = True
                try:
                    if not dbbckp.dump_call.poll():
                        dbbckp.dump_call.kill()
                except AttributeError:
                    pass
                try:
                    if not dbbckp.arch_call.poll():
                        dbbckp.arch_call.kill()
                except AttributeError:
                    pass
            if config['cron_fs']:
                fsbckp.EOFFlag = True
            self.min_bee.wait()
            try:
                self.delete_db_bckp_data(config)
                self.logger.info('-------БЭКАП ПРЕРВАН | ФАЙЛЫ УДАЛЕНЫ-------')
            except Exception as e:
                self.logger.error(f'P.S. Ошибка удаления записи о бэкапах в БД {e}')
                self.logger.info('-------ПРЕРЫВАНИЕ БЭКАПА С ОШИБКОЙ-------')

            handlers = self.logger.handlers[:]
            for handler in handlers:
                self.logger.removeHandler(handler)
                handler.close()
            self.delete_files_stp()
            # При наличии ошибок в отчёте, высылаем его письмом по почте
            check_log_file = open(config['path_to_backups'] + "/logs/" + config['latest_run'] + ".log", 'r')
            check_log = check_log_file.read()
            check_log_file.close()
            if config['mail_alerts']:
                if 'WARNING' in check_log or 'ERROR' in check_log or 'CRITICAL' in check_log:
                    try:
                        mail.run()  # Отсылаем отчёт письмом
                    except Exception:
                        pass
            self.stp_inf = self.messageCI.exec()
            self.close()

# Стили
qss = f""" 
        QLineEdit {{
            padding-left: 2px;
        }}
        QPushButton::menu-indicator {{
            image: url('{resource_path("assets/arrow-b.png")}');
            subcontrol-position: right center;
            subcontrol-origin: padding;
            width: 10px;
            height: 10px;
            left: -4px; 
        }}
        QMenu::item:selected {{
            background-color: rgb(0, 105, 89);
        }}
        QGroupBox {{
            background-color: transparent;
            border: 1px solid #b5b5b5;
            border-radius: 8px;
            margin-top: 8px;
        }}
        QGroupBox::title {{
            subcontrol-origin: margin;
            subcontrol-position: top center;
            padding: 0px 10px;
        }}
        QTabBar::tab {{
            background-color: #ffffff;
            border: 0.5px solid #ededed;
            padding: 3px 5px;
            width: 65px;
        }}
        QTabBar::tab:selected {{
            background-color: rgb(0, 105, 89);
            color: #ffffff;
        }}
        QComboBox {{
            border-radius: 5.5px;
            padding-left: 5px;
            margin-top: 1.5px;
        }}
        QComboBox:editable {{
            background: #ffffff;
        }}
        QComboBox QAbstractItemView {{
            padding: 1px;
            margin: 1px, 1px, 1px, 1px;
        }}
        QComboBox::drop-down {{
            background-color: rgb(0, 105, 89); 
            border-top-right-radius: 5px;
            border-bottom-right-radius: 5px;
            width: 22px;
            margin: 0;
            padding: 0;
        }}
        QComboBox::down-arrow {{
            image: url('{resource_path("assets/arrow.png")}');
            width: 10px;
            height: 10px;
        }}
        QListView {{
            background-color: rgb(255, 255, 255);
            selection-background-color: rgb(0, 105, 89);
        }}
        QLabel#tooltip {{
            font-size: 9px;
            color: #787878;
        }}
        QPushButton#trblshtng {{
            border: 1px solid #ffffff;
            background-color:rgb(0, 105, 89);
            color: #ffffff;
        }}
        QPushButton#trblshtng:hover {{
            background-color:rgb(48, 128, 114);
        }}
        QPushButton#trblshtng:pressed {{
            color: #008f79;
            background-color:rgb(48, 128, 114);
        }}
        QPushButton#log_btn {{
            border-radius: 100%;
        }}
        QCheckBox::indicator {{
            width: 15px;
            height: 15px;
            border: 0.5px solid #c9c9c9;
            border-radius: 2px;
            background-color: rgb(255, 255, 255);
            margin: 2px;
        }}
        QCheckBox::indicator:checked {{
            background-color: rgb(0, 105, 89);
            image: url('{resource_path("assets/check.png")}');
        }}
        QTabWidget::tab-bar {{
            alignment: center;
        }}
      """

if __name__ == "__main__":
    app = QApplication(sys.argv)
    # app.setWindowIcon(QIcon(resource_path("assets/marmot_logo.png")))
    app.setStyleSheet(qss)
    if len(sys.argv) == 2 and sys.argv[1] == 'CRON':
        cron_surok = CRON_Widget()
        cron_surok.show()
    else:
        surok_app = SUROK_Admin()
        surok_app.show()
    sys.exit(app.exec())