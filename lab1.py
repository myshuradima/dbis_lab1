# -*- coding: utf8 -*-
import psycopg2
import os
from queries import cteate_table, insert_string, ball_list, select_query
from config import dbname, password, port, username, host, N, way1, way2


def func1(str_list):
    new_list = []
    for el in str_list:
        try:
            if el[0] == '"':
                el = el[1:-1]
                el = el.replace('""', '"')
                new_list.append(el)
            else:
                new_list.append(el)
        except:
            print(str_list)
    return new_list




counter1 = 0



def upload_files_to_db():
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=username,
        password=password
    )
    cur = conn.cursor()
    cur.execute(cteate_table)
    cur.close()
    conn.commit()
    conn.close()
    if not os.listdir(path="dir"):
        with open(way1, encoding='cp1251') as file:
            a = file.readline()
            print(a)
            i = 0
            k = 0
            f = open('dir/partfile1_' + str(k) + '.csv', 'w')
            while len(a) > 50:
                if i < N:
                    a = file.readline()
                    a = a[0:-1] + ";2019\n"
                    f.write(a)
                    i = i + 1
                else:
                    f.close()
                    i = 0
                    k = k + 1
                    f = open('dir/partfile1_' + str(k) + '.csv', 'w')
            f.close()

        with open(way2, encoding='cp1251') as file:
            a = file.readline()
            print(a)
            i = 0
            k = 0
            f = open('dir/partfile2_' + str(k) + '.csv', 'w')
            while len(a) > 50:
                if i < N:
                    a = file.readline()
                    a = a[0:-2] + ";2020\n"
                    f.write(a)
                    i = i + 1
                else:
                    f.close()
                    i = 0
                    k = k + 1
                    f = open('dir/partfile2_' + str(k) + '.csv', 'w')
            f.close()
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=username,
            password=password
        )
        for el in os.listdir(path="dir"):
            print("Uploading:", el)

            with open("dir/" + el, encoding="cp1251") as file:
                a = True
                while a and a != '':
                    a = file.readline()
                    a = a.rstrip("\n")
                    a_list = a.split(';')
                    if a_list != ['']:
                        a_list = func1(a_list)
                    i = 0

                    while i < len(a_list):
                        if a_list[i] == "null":
                            a_list[i] = None
                        elif i in ball_list:
                            a_list[i] = float(a_list[i].replace(',', '.'))
                        i = i + 1

                    if len(a_list) > 20:
                        cur = conn.cursor()
                        cur.execute(insert_string, a_list)
                        cur.close()

            conn.commit()
            print("Done:", el)
            os.remove("dir/" + el)
        conn.close()
        return 1
    except:
        print("Failed connection with database, please restore connection and restart the program")
        return 0



def select_from_db():
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=username,
        password=password
    )
    cur = conn.cursor()
    cur.execute(select_query)
    records = cur.fetchall()
    print(type(records))
    cur.close()
    conn.commit()
    conn.close()
    with open("result.csv", 'w') as file:
        file.write("Область, Середній бал ЗНО, Середній бал ДПА, Рік\n")
        for el in records:
            print(el)
            new_string = ""
            new_string = new_string + '"' + el[0] + '",' + str(round(el[1], 2)) + ',' + \
                         str(round(el[2], 2)) + ',' + str(el[3] + '\n')
            file.write(new_string)


if not os.path.exists(path="dir"):
    os.mkdir(path="dir")
a = input("press u to upload files to db and s to select data from db ")
if a == 'u':
    b = upload_files_to_db()
    if b == 1:
        select_from_db()
else:
    select_from_db()

