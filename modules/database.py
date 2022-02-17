import mysql.connector
import time
import modules.config
from mysql.connector import Error, errorcode


class Database:
    ### MySQL ###
    def __init__(self, params_bd=''):
        if params_bd:
           for v_db in params_bd:
                  if  v_db[0] == "host":
                      host=v_db[1]
                  else:
                      host=modules.config.Mysql_host
                  if  v_db[0] == "port":
                      port=v_db[1]
                  else:
                      port=modules.config.Mysql_port
                  if  v_db[0]=="username":
                      user=v_db[1]
                  else:
                      user=modules.config.Mysql_user
                  if  v_db[0]=="password":
                      password=v_db[1]
                  else:
                      password=modules.config.Mysql_password
                  if  v_db[0]=="database":
                      database= v_db[1]
                  else:
                      database=modules.config.Mysql_database
       
           self.Mysql_database_config={"host":host,
                                    "port":port,
                                    "user":user, 
                                    "password":password, 
                                    "database":database}   
        else:    
           self.Mysql_database_config = {"host": modules.config.Mysql_host,
                                      "port": modules.config.Mysql_port,
                                      "user": modules.config.Mysql_user,
                                      "password": modules.config.Mysql_password,
                                      "database": modules.config.Mysql_database}

        ### Проверка на NULL значения msg_is_usseful ###

    def chek_sorting_telegram_message(self, name_tb="articles_telegramm"):
        db_config = self.Mysql_database_config
        try:
            conn = mysql.connector.MySQLConnection(**db_config)
            if conn.is_connected():
                mycursor = conn.cursor()
                sql = "Select Count(*) from " + str(name_tb) + \
                    " where msg_is_usseful IS NULL"
                mycursor.execute(sql)
                result_sql = mycursor.fetchone()
                for res_tupl in result_sql:
                    if res_tupl > 0:
                        return 1
                    else:
                        return 0
                return result_sql
        except Error as e:
            if e.errno == errorcode.ER_BAD_DB_ERROR:
                return 0
            pass

        ### Получить одно сообщение из бд###
    def get_telegram_message(self, name_tb="articles_telegramm"):
        db_config = self.Mysql_database_config
        try:
            conn = mysql.connector.MySQLConnection(**db_config)
            if conn.is_connected():
                mycursor = conn.cursor()
                sql = "Select id, text from  " + str(name_tb) +\
                    " where msg_is_usseful IS NULL limit 10"
                mycursor.execute(sql)
                result_text = mycursor.fetchall()
                return result_text
            conn.commit()
        except Error as e:
            print(e)

    ### Добавляем данные из tellegram канала в БД###
    def save_result_telegram_to_db(self, message_telegram, name_table_bd="articles_telegramm"):
        db_config = self.Mysql_database_config
        try:
            conn = mysql.connector.MySQLConnection(**db_config)
        except mysql.connector.Error as e:
            if e.errno == errorcode.CR_CONN_HOST_ERROR:
                print("Добавление данных в базу прервано,так как отсутствует соединение с базой."
                      "Проверьте соеденение к вашей базе и повторите попытку!")
                return 0
            if e.errno == errorcode.ER_BAD_DB_ERROR:
                self.create_new_db_schem()
                result_create_bd = self.create_new_db_table(
                    db_config, name_table_bd)
                conn = mysql.connector.MySQLConnection(**db_config)
            if result_create_bd != 1:
                dublicate = self.check_dublicate(name_table_bd)
                if dublicate != 1:
                    self.delet_dublicate(dublicate, name_table_bd)

        mycursor = conn.cursor()
        sql = "INSERT IGNORE  INTO " + str(name_table_bd) +\
            " (chanel_id,text,adddata_msg,msg_id,chanel_name)"\
            "VALUES(%s,%s,%s,%s,%s)"
        val = []
        for elem in message_telegram:
            res_id_chenal = elem[0]
            res_text = elem[1]
            data = elem[2]
            id_mesage = elem[3]
            chanel_name = elem[4]
            val.append((res_id_chenal, res_text, data, id_mesage, chanel_name))
        try:
            mycursor.executemany(sql, val)
            conn.commit()
            print('Message add to database!')
        except mysql.connector.IntegrityError:
            pass

 ### Устанавливаем фильтрацию сообщений###
    def set_sorting_msage_to_db(self, v_usseful, name_tb="articles_telegramm"):
        db_config = self.Mysql_database_config
        try:
            conn = mysql.connector.MySQLConnection(**db_config)
            if conn.is_connected():
                mycursor = conn.cursor()
                for elem in v_usseful:
                    res_id = elem[0]
                    res_is_usseful = elem[1]
                    sql = "Update " + str(name_tb) + " set msg_is_usseful =" + \
                        str(res_is_usseful) + " where id= "+str(res_id)
                    mycursor.execute(sql)
            conn.commit()
            print('set sorting the database!')
        except Error as e:
            print(e)

        ### Обновление данных ###
    def update_articles_to_db(self, id, name_tb):
        db_config = self.Mysql_database_config
        con = mysql.connector.MySQLConnection(**db_config)
        mycursor = con.cursor()
        sql = "Update  " + str(name_tb) + " set updatedatetime = " + \
            str(time.time())+" where id = " + str(id)
        mycursor.execute(sql)
        con.commit()

    def create_new_db_schem(self):
        new_conn = None
        try:
            new_conn = mysql.connector.connect(host=self.Mysql_database_config["host"],
                                               user=self.Mysql_database_config["user"],
                                               password=self.Mysql_database_config["password"])
        except mysql.connector.Error as e:
            if e.errno == errorcode.CR_CONN_HOST_ERROR:
                print(
                    "Не возможно создать новую схему, так как отсутствует соединение с базой!")
                return 0
        create_db_query = "CREATE DATABASE " + \
            self.Mysql_database_config["database"] + \
            " CHARACTER SET utf8 COLLATE utf8_general_ci"
        with new_conn.cursor() as cursor_cdb:
            cursor_cdb.execute(create_db_query)

    def create_new_db_table(self, config_db, name_tb):

        conn_create_table = None
        try:
            conn_create_table = mysql.connector.connect(
                **config_db, charset="utf8", use_unicode=True)
        except mysql.connector.Error as e:
            if e.errno == errorcode.CR_CONN_HOST_ERROR:
                print(
                    "Не возможно создать нвую таблицу, так как отсутствует соединение к базе!")
                return 0

        create_reviewers_table_query = """
               CREATE TABLE  """ + str(name_tb) + """  (
               id INT AUTO_INCREMENT PRIMARY KEY,
               msg_id INT(10) NOT NULL UNIQUE,
               chanel_id INT(11) NOT NULL,
               text TEXT,
               chanel_name TEXT,
               adddata_msg DATETIME,
               updatedata DATETIME,
               msg_is_usseful int(11) DEFAULT NULL
              )
               """
        with conn_create_table.cursor() as cursor_ct:
            cursor_ct.execute(create_reviewers_table_query)
        return 1

    def check_dublicate(self, name_tb):
        db_config = self.Mysql_database_config
        try:
            conn = mysql.connector.MySQLConnection(**db_config)
            if conn.is_connected():
                mycursor = conn.cursor()
                sql = "Select id ,msg_id, chanel_id from " + str(name_tb) +\
                    "  group by msg_id, chanel_id having count(*)>1"
                mycursor.execute(sql)
                result_dublicate = mycursor.fetchall()
                if result_dublicate == []:
                    return 1
                else:
                    return result_dublicate
            conn.commit()
        except Error as e:
            print(e)

    def delet_dublicate(self, list_dublicate, name_tb):
        db_config = self.Mysql_database_config
        try:
            conn = mysql.connector.MySQLConnection(**db_config)
            if conn.is_connected():
                mycursor = conn.cursor()
                for elem in list_dublicate:
                    id = elem[0]
                    sql = "Delete from "+str(name_tb) +\
                        " where id="+id
                    mycursor.execute(sql)
            conn.commit()
        except Error as e:
            print(e)
