# -*- coding: utf-8 -*-
from timeit import default_timer as timer
import datetime as dt
import fdb
import logging
import os
import pandas as pd
import sqlalchemy as sa
import sys

from config import DB


logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler(
            filename='uploader.log', encoding='utf-8', mode='a+'
            )
        ],
    datefmt='%d-%m-%Y %H:%M:%S',
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
)

query_data = sa.text("""
SELECT
    DISTINCT ID_IN_OUT,
    STAFF_ID,
    FULL_FIO,
    LIST(TYPE_POST) as TYPE_POST,
    FULL_POST,
    LIST(SUBDIVISION) as SUBDIVISION,
    CHAIR,
    STUDENT_GROUP,
    BIRTHDAY_DATE,
    AREA,
    DATE_PASS,
    TIME_PASS,
    TYPE_PASS
FROM
    (
        SELECT
            TI.ID_TB_IN as ID_IN_OUT,
            TI.STAFF_ID,
            S.FULL_FIO,
            AR.DISPLAY_NAME as TYPE_POST,
            FP.INFO_DATA as FULL_POST,
            SUBREF.DISPLAY_NAME as SUBDIVISION,
            KAF.INFO_DATA as CHAIR,
            GR.INFO_DATA as STUDENT_GROUP,
            SD.BIRTHDAY_DATE,
            ATR.DISPLAY_NAME AS AREA,
            TI.DATE_PASS,
            TI.TIME_PASS,
            TI.TYPE_PASS
        FROM
            TABEL_INTERMEDIADATE as TI
            JOIN STAFF as S ON TI.STAFF_ID = S.ID_STAFF -- для получения полного имени
            JOIN STAFF_DOCS as SD ON TI.STAFF_ID = SD.STAFF_ID -- для получения даты рождения
            JOIN AREAS_TREE AS ATR ON TI.AREAS_TREE_ID = ATR.ID_AREAS_TREE -- для получения места, куда осуществлялся вход
            JOIN (
                SELECT
                    DISTINCT STAFF_ID,
                    APPOINT_ID,
                    SUBDIV_ID
                FROM
                    STAFF_REF
            ) AS STREF ON TI.STAFF_ID = STREF.STAFF_ID -- для получения APPOINT_ID для каждого STAFF_ID, чтобы в дальнейшем получить тип "должности"
            JOIN APPOINT_REF AS AR ON STREF.APPOINT_ID = AR.ID_REF -- для получения типа посетителя (типа "должности")
            JOIN (
                SELECT
                    STAFF_ID,
                    INFO_DATA
                FROM
                    STAFF_INFO_DATA_STR
                WHERE
                    REF_ID = 185672
            ) as KAF ON TI.STAFF_ID = KAF.STAFF_ID -- для получение кафедры кафедры; информация по REF_ID содержится в таблице STAFF_INFO_REF
            JOIN (
                SELECT
                    STAFF_ID,
                    INFO_DATA
                FROM
                    STAFF_INFO_DATA_STR
                WHERE
                    REF_ID = 185673
            ) as GR ON TI.STAFF_ID = GR.STAFF_ID -- для получения группы студента
            JOIN (
                SELECT
                    STAFF_ID,
                    INFO_DATA
                FROM
                    STAFF_INFO_DATA_STR
                WHERE
                    REF_ID = 245556
            ) as FP ON TI.STAFF_ID = FP.STAFF_ID -- для получения "полного" названия должности
            JOIN SUBDIV_REF as SUBREF ON STREF.SUBDIV_ID = SUBREF.ID_REF -- для получения подразделения
        WHERE
            TI.ID_TB_IN > :lstid
    ) r
GROUP BY
    ID_IN_OUT,
    STAFF_ID,
    FULL_FIO,
    FULL_POST,
    CHAIR,
    STUDENT_GROUP,
    BIRTHDAY_DATE,
    AREA,
    DATE_PASS,
    TIME_PASS,
    TYPE_PASS
""")

query_max_id = sa.text(
"""
    SELECT 
        MAX(id_in_out) 
    FROM 
        skud_data
""")

query_log = sa.text(
"""
    INSERT INTO
        skud_upload_log (exit_point, message, is_failure, timestamp_exec)
    VALUES
        (:ep, :msg, :ifail, :te)
"""
)

def main():
    logging.info("Начало работы...")
    # создаем engine для взаимодействия с БД НГТУ
    try:
        engine_oracle = sa.create_engine(
            f"{DB['name']}+{DB['driver']}://{DB['username']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['section']}",
            fast_executemany=True,
            echo=False
        )
        engine_oracle.connect()
    except Exception as e:
        s1 = "Не удалось подключиться к БД ЦИУ."
        logging.error(s1 + ' ' + str(e))
        sys.exit(1)
    logging.info("Engine для работы с БД ЦИУ успешно создан")

    # создаем engine для взаимодействия с БД СКУД
    try:
        engine_firebird = sa.create_engine(
            'firebird+fdb://sysdba:masterkey@localhost/D:/YandexDisk/work/skud/db.fdb?charset=UTF-8',
            echo=False
        )
        engine_firebird.connect()
    except Exception as e:
        s2 = "Не удалось подключиться к БД СКУД"
        logging.error(s2 + ' ' + str(e))
        engine_oracle.execute(
            query_log.bindparams(ep=2, msg=s2, ifail=1, te=dt.datetime.today())
        )
        sys.exit(2)
    logging.info("Engine для работы с БД СКУД успешно создан")

    # получаем максимальный ID_IN_OUT из таб. SKUD_DATA из БД ЦИУ
    try:
        with engine_oracle.connect() as conn:
            with conn.begin():
                max_id_in_out = engine_oracle.execute(
                    query_max_id
                    ).fetchone()[0]
    except Exception as e:
        s3 = "Не удалось получить максимальный ID_IN_OUT из БД ЦИУ"
        logging.error(s3 + ' ' + str(e))
        engine_oracle.execute(
            query_log.bindparams(ep=3, msg=s3, ifail=1, te=dt.datetime.today())
        )
        sys.exit(3)
    logging.info("Максимальный ID_IN_OUT из таб. SKUD_DATA успешно получен.")

    logging.info("Получение данных из БД СКУД началось...")
    # получаем необходимые данные из БД СКУД
    try:
        with engine_firebird.connect() as conn:
            with conn.begin():
                data = engine_firebird.execute(
                    query_data.bindparams(lstid=max_id_in_out))
    except Exception as e:
        s4 = "Не удалось получить данные из БД СКУД."
        logging.error(s4 + ' ' + str(e))
        engine_oracle.execute(
            query_log.bindparams(ep=4, msg=s4, ifail=1, te=dt.datetime.today())
        )
        sys.exit(4)
    logging.info("Получение данных из БД СКУД успешно завершено.")

    #  формируем DataFrame, если были получены какие-то данные
    data_fetch = data.fetchall()
    if len(data_fetch):
        df = pd.DataFrame(
            data_fetch,
            columns=[
                'id_in_out',
                'skud_id_stuff',
                'full_fio',
                'type_post',
                'full_post',
                'subdivision',
                'chair',
                'student_group',
                'birthday_date',
                'area',
                'date_pass',
                'time_pass',
                'type_pass',
            ]
        )

        # из даты и времение получаем timestamp
        df['timestamp_pass'] = df.apply(
            lambda row: dt.datetime.combine(
                row['date_pass'], row['time_pass']),
            axis=1
        )
        df = df.drop(columns=['date_pass', 'time_pass'])
        print(df)
    else:
        s_0 = "Новых записей в БД СКУД не найдено."
        logging.info(s_0)
        engine_oracle.execute(
            query_log.bindparams(ep=0, msg=s_0, ifail=0, te=dt.datetime.today())
        )
        sys.exit(0)
    logging.info("Полученнные данные из БД СКУД успешно преобразованы.")

    logging.info("Выгрузка данных в БД ЦИУ началась...")
    # выгрузка skud_data в БД НГТУ
    try:
        object_columns = [col for col in df.columns[df.dtypes == 'object'].tolist()]
        dtypes = {object_col: sa.types.VARCHAR(1000) for object_col in object_columns}
        df.to_sql(
            name='skud_data',
            con=engine_oracle,
            schema='PERCOUSER',
            if_exists='append',
            index=False,
            index_label=None,       # TODO: определить множество индексов таблицы
            chunksize=5000,
            method=None,             # ! 11 версия Oracle не поддерживает 'multi' :(
            dtype=dtypes
        )
    except Exception as e:
        s5 = "Не удалось выгрузить данные в БД ЦИУ."
        logging.error(s5 + ' ' + str(e))
        engine_oracle.execute(
            query_log.bindparams(ep=0, msg=s5, ifail=1, te=dt.datetime.today())
        )
        sys.exit(5)

    # сообщение об успешном завершении работы
    s0 = "Новые данные из БД СКУД успешно выгружены в БД ЦИУ."
    logging.info(s0)
    engine_oracle.execute(
        query_log.bindparams(ep=0, msg=s0, ifail=0, te=dt.datetime.today())
    )
    sys.exit(0)


if __name__ == "__main__":
    start = timer()
    main()
    end = timer()
    s_time = f"Время выполнения программы: {end - start} секунд"
    logging.info(s_time)
