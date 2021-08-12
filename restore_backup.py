# -*- coding: utf-8 -*-
import fdb

from config import DLL_PATH


def report_progress(line):
    """Help-ер для вывода процесса restore-а в консоль (можно без него)"""
    print(line)


def main():
    fdb.load_api(DLL_PATH)
    svc = fdb.services.connect(user='sysdba', password='masterkey')
    svc.restore('Backup1.fbk', 'db.fdb', callback=report_progress)


if __name__ == '__main__':
    main()
