Скрипт для получения данных о проходах из БД СКУД в БД информационнй системы.

###Таблицы в percouser

1. skud_upload_log – лог выгрузки данных из скуд. Поля: id, exit_point, message, is_failure, timestamp_exec

2. skud_data - данные по посещении физлиц геобъектов НГТУ. Поля: id_in_out, skud_id_stuff, full_fio, type_post, full_post, subdivision, chair, student_group, birthday_date, area, timestamp_pass, type_pass

3. skud_staff - информация о пользователях СКУД. Поля: id_staff, full_fio, birthday, id_card, fk_student, id_person
