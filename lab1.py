# -*- coding: utf8 -*-
import psycopg2
import urllib.request
import py7zr
import os
from queries import cteate_table, insert_string, ball_list, select_query
N = 2000

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='postgres',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()
cur.execute(cteate_table)
cur.close()
conn.commit()
conn.close()
#way1 = 'https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO2019.7z'
#way2 = 'https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO2020.7z'
#urllib.request.urlretrieve(way1, '2019.7z')
#urllib.request.urlretrieve(way2, '2020.7z')

#with py7zr.SevenZipFile('2019.7z', 'r') as file:
#    file.extractall()
#with py7zr.SevenZipFile('2020.7z', 'r') as file:
#    file.extractall()




print(len(os.listdir(path="dir")))


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
    if not os.listdir(path="dir"):
        with open('Odata2019File.csv', encoding='cp1251') as file:
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

        with open('Odata2020File.csv', encoding='cp1251') as file:
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
            host='localhost',
            port=5432,
            dbname='postgres',
            user='postgres',
            password='postgres'
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
        host='localhost',
        port=5432,
        dbname='postgres',
        user='postgres',
        password='postgres'
    )
    cur = conn.cursor()
    cur.execute(select_query)
    records = cur.fetchall()
    print(type(records))
    cur.close()
    conn.commit()
    conn.close()
    with open("result.csv", 'w') as file:
        for el in records:
            new_string = ""
            new_string = new_string + '"' + el[0] + '",' + str(round(el[1], 2)) + ',' + \
                         str(round(el[2], 2)) + ',' + str(el[3] + '\n')
            file.write(new_string)


a = input("press u to upload files to db and s to select")
if a == 'u':
    upload_files_to_db()
else:
    select_from_db()

