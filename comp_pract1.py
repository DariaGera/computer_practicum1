import psycopg2
import psycopg2.errorcodes
from config import config
import csv

# 5 варіант
# Порівняти середній бал з Історії України у кожному регіоні у 2020 та 2019 роках серед тих, кому було зараховано тест


def sub_files(file, year):
    amount = 15
    with open(file, 'r') as f:
        f.readline()
        csv_file = f.readlines()
        file_len = len(csv_file)
        rows = file_len//amount + 1
        filename_list = []
        filename = 1
        for i in range(file_len):
            if i % rows == 0:
                name = str(filename) + '.'+str(year)+'.csv'
                sub = open(str(filename) + '.'+str(year)+'.csv', 'w+')
                sub.writelines(csv_file[i:i+rows])
                filename_list += [name]
                filename += 1
                sub.close()
    return filename_list


def change_file(lst, year):
    for file_name in lst:
        file = open(file_name)
        reader = csv.reader(file, delimiter=';', quoting=csv.QUOTE_ALL)
        rows = []
        for row in reader:
            row.insert(0, year)
            for i in [19, 30, 40, 50, 60, 70, 80, 89, 99, 109, 119]:
                row[i] = row[i].replace(',', '.')
            string = ';'.join(row)
            rows.append(string)
        file.close()

        with open(file_name, 'w', newline='') as f:
            for row in rows:
                f.write(row + '\n')
        print("file changed "+file_name)


def create_table(conn):
    query1 = '''CREATE TABLE zno_ua19_20(
                ZNO_Year INTEGER,
                OUTID	TEXT,
                Birth	INTEGER,
                SEXTYPENAME	TEXT,
                REGNAME	TEXT,
                AREANAME	TEXT,
                TERNAME	TEXT,
                REGTYPENAME	TEXT,
                TerTypeName	TEXT,
                ClassProfileNAME	TEXT,
                ClassLangName	TEXT,
                EONAME	TEXT,
                EOTYPENAME	TEXT,
                EORegName	TEXT,
                EOAreaName	TEXT,
                EOTerName	TEXT,
                EOParent	TEXT,
                UkrTest	TEXT,
                UkrTestStatus	TEXT,
                UkrBall100	NUMERIC, 
                UkrBall12	INTEGER,
                UkrBall	INTEGER,
                UkrAdaptScale	TEXT,
                UkrPTName	TEXT,
                UkrPTRegName	TEXT,
                UkrPTAreaName	TEXT,
                UkrPTTerName	TEXT,
                histTest	TEXT,
                HistLang	TEXT,
                histTestStatus	TEXT,
                histBall100	NUMERIC, 
                histBall12	INTEGER,
                histBall	INTEGER,
                histPTName	TEXT,
                histPTRegName	TEXT,
                histPTAreaName	TEXT,
                histPTTerName	TEXT,
                mathTest	TEXT,
                mathLang	TEXT,
                mathTestStatus	TEXT,
                mathBall100	NUMERIC, 
                mathBall12	INTEGER,
                mathBall	INTEGER,
                mathPTName	TEXT,
                mathPTRegName	TEXT,
                mathPTAreaName	TEXT,
                mathPTTerName	TEXT,
                physTest	TEXT,
                physLang	TEXT,
                physTestStatus	TEXT,
                physBall100	NUMERIC, 
                physBall12	INTEGER,
                physBall	INTEGER,
                physPTName	TEXT,
                physPTRegName	TEXT,
                physPTAreaName	TEXT,
                physPTTerName	TEXT,
                chemTest	TEXT,
                chemLang	TEXT,
                chemTestStatus	TEXT,
                chemBall100	NUMERIC, 
                chemBall12	INTEGER,
                chemBall	INTEGER,
                chemPTName	TEXT,
                chemPTRegName	TEXT,
                chemPTAreaName	TEXT,
                chemPTTerName	TEXT,
                bioTest	TEXT,
                bioLang	TEXT,
                bioTestStatus	TEXT,
                bioBall100	NUMERIC, 
                bioBall12	INTEGER,
                bioBall	INTEGER,
                bioPTName	TEXT,
                bioPTRegName	TEXT,
                bioPTAreaName	TEXT,
                bioPTTerName	TEXT,
                geoTest	TEXT,
                geoLang	TEXT,
                geoTestStatus	TEXT,
                geoBall100	NUMERIC, 
                geoBall12	INTEGER,
                geoBall	INTEGER,
                geoPTName	TEXT,
                geoPTRegName	TEXT,
                geoPTAreaName	TEXT,
                geoPTTerName	TEXT,
                engTest	TEXT,
                engTestStatus	TEXT,
                engBall100	NUMERIC, 
                engBall12	INTEGER,
                engDPALevel	TEXT,
                engBall	INTEGER,
                engPTName	TEXT,
                engPTRegName	TEXT,
                engPTAreaName	TEXT,
                engPTTerName	TEXT,
                fraTest	TEXT,
                fraTestStatus	TEXT,
                fraBall100	NUMERIC, 
                fraBall12	INTEGER,
                fraDPALevel	TEXT,
                fraBall	INTEGER,
                fraPTName	TEXT,
                fraPTRegName	TEXT,
                fraPTAreaName	TEXT,
                fraPTTerName	TEXT,
                deuTest	TEXT,
                deuTestStatus	TEXT,
                deuBall100	NUMERIC, 
                deuBall12	INTEGER,
                deuDPALevel	TEXT,
                deuBall	INTEGER,
                deuPTName	TEXT,
                deuPTRegName	TEXT,
                deuPTAreaName	TEXT,
                deuPTTerName	TEXT,
                spaTest	TEXT,
                spaTestStatus	TEXT,
                spaBall100	NUMERIC,
                spaBall12	INTEGER,
                spaDPALevel	TEXT,
                spaBall	INTEGER,
                spaPTName	TEXT,
                spaPTRegName	TEXT,
                spaPTAreaName	TEXT,
                spaPTTerName	TEXT
    )'''
    query2 = '''CREATE TABLE success_commit(id SERIAL, file_name TEXT)'''
    cursor = conn.cursor()
    cursor.execute(query1)
    cursor.execute(query2)
    conn.commit()
    cursor.close()
    print("tables have been created!")


def insert_data(lst, conn):
    cursor = conn.cursor()
    inserted = False

    while not inserted:
        try:
            for file_name in lst:
                cursor.execute("BEGIN")
                file = open(file_name)
                cursor.copy_from(file, '"zno_ua19_20"', sep=';', null='null')
                cursor.execute("""insert into success_commit(file_name) values(%s)""", (file_name,))
                cursor.execute("COMMIT")
                file.close()

                cursor.execute("""select file_name from success_commit order by id desc limit 1""")
                record = cursor.fetchone()
                if record[0] != file_name:
                    inserted = False
                    conn.rollback()
                elif record[0] == lst[-1]:
                    inserted = True
                print("insert success"+file_name)

        except psycopg2.OperationalError as e:
            conn.rollback()
            # якщо з'єднання з базою даних втрачено
            if e.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
                print("База даних впала -- чекаємо на відновлення з'єднання...")
                connection_restored = False
                while not connection_restored:
                    try:
                        params = config()
                        conn = psycopg2.connect(**params)
                        conn.autocommit = False
                        cursor = conn.cursor()
                        connection_restored = True
                    except psycopg2.OperationalError:
                        pass
                print("З'єднання відновлено!")

        except (Exception, psycopg2.DatabaseError) as error:
            print("Помилка в транзакції", error)
            conn.rollback()


def write_result(conn):
    cursor = conn.cursor()
    with open("Task_var5.csv", "w", newline="") as file:
        writer = csv.writer(file)
        query_task = """
        SELECT  regname, zno_year, histteststatus, AVG(histball100)  AS avgball
        FROM public.zno_ua19_20 
        GROUP BY regname, zno_year, histteststatus
        HAVING histteststatus = 'Зараховано'  
        """
        cursor.execute(query_task)
        writer.writerow(["regname", "zno_year", "histteststatus", "avgball"])
        for regname, zno_year, histteststatus, avgball in cursor:
            writer.writerow([regname, zno_year, histteststatus, avgball])
    cursor.close()


def connect():
    """ Connect to the PostgreSQL database server """
    connection = None
    try:
        """ Connect to the PostgreSQL database server """
        params = config()
        # connect to the PostgreSQL server
        connection = psycopg2.connect(**params)
        connection.autocommit = False
        # create a cursor
        cur = connection.cursor()

        # якщо таблиця вже існує -- видаляємо її
        cur.execute('DROP TABLE IF EXISTS zno_ua19_20;')
        cur.execute('DROP TABLE IF EXISTS success_commit')
        connection.commit()
        create_table(connection)

        l_2019 = sub_files('Odata2019File.csv', 2019)
        l_2020 = sub_files('Odata2020File.csv', 2020)

        change_file(l_2019, '2019')
        change_file(l_2020, '2020')

        insert_data(l_2019, connection)
        insert_data(l_2020, connection)

        write_result(connection)
        print("result has just been written")
        cur.close()
    except psycopg2.OperationalError:
        print("OperationalError")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print("Database connection closed.")


if __name__ == '__main__':
    connect()


# Конфігурація з'єднання з БД має виконуватись без внесення змін у код програми
'''
By using the database.ini, you can change the PostgreSQL connection parameters
when you move the code to the production environment without modifying the code.
Notice that if you git, you need to add the database.ini to the .gitignore file to not
committing the sensitive information to the public repo like github.
'''






