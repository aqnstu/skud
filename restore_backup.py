# -*- coding: utf-8 -*-
import fdb

def report_progress(line):
    print(line)


def main():
    fdb.load_api('C:/Program Files/Firebird/Firebird_3_0/fbclient.dll')
    svc = fdb.services.connect(user='sysdba', password='masterkey')
    svc.restore('backup.fbk', 'db.fdb', callback=report_progress)

if __name__ == '__main__':
    main()
