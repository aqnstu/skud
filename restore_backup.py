# -*- coding: utf-8 -*-
import fdb

from config import LOCAL_DB, DLL_PATH, DB_DUMP_FILENAME, DB_FILENAME


def report_progress(line):
    """Help-ер для вывода процесса restore-а в консоль (можно без него)"""
    print(line)


def main():
    fdb.load_api(DLL_PATH)
    svc = fdb.services.connect(user=LOCAL_DB['username'],
                               password=LOCAL_DB['password'])
    svc.restore(DB_DUMP_FILENAME, DB_FILENAME, callback=report_progress)

if __name__ == '__main__':
    main()
