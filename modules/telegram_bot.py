import datetime
import re
import time
import argparse
import sys
from datetime import timedelta

from telethon.sync import TelegramClient

import modules.config
import modules.database



###Client Telegram###
def Create_Client_Telegram(sesion_name,api_id,api_hash):
  client = TelegramClient(sesion_name, api_id, api_hash)    
  return client


# поиск канала в telegramе
def search_chanal_in_to_telegram(name_chanal,client):
    try:
        my_channel = client.get_entity(name_chanal)
        if my_channel is not None:
            chanal_id = my_channel.id
        else:
            chanal_id = 0

    except ValueError:
        chanal_id = 0

    return chanal_id


def list_telegram_message(client,chanel_name, chanal_id, start_data, end_date):

    telegram_list = []
    message_counter = 0

    # использую функцию iter_messages, так как get_messages выводит только по  одному сообщению.
    # limit - пораметр, регулирующий колличество  сообщений.
    # offset_date-дата смещения (будут извлечены сообщения, предшествующие этой дате)
    # Так как в функциях iter_messages  и get_messages
    # не предусмотренно пораметра, что бы задать временной отрезок получения сообщений, я высчитываю разницу между датами и
    #  использую в качестве значения limit, возможно я не правильно делаю.
    # Есть возможность задать обратнвй вывод сообщений (от самого старого к самому новому относительно пораметра offset_date),
    # установив пораметр reverse=True
    for message in client.iter_messages(chanal_id, offset_date=start_data):

        id_message = message.id

        if message.message is None:
            new_message = ""
        else:
            new_message = message.message

        if message.date is None:
            message_date = None
        else:
            # преобразование даты сообщения
            message_date = message.date - timedelta(seconds=time.timezone)
            message_date = message_date.replace(tzinfo=None)

        if end_date == '':
            if datetime.date.today() == message_date.date():
                if message_counter < 50:
                    if new_message != "":
                        telegram_list.append(
                            [chanal_id, new_message, message_date, id_message, chanel_name])
                        message_counter += 1
                else:
                    break
            else:
                before_yesterday = datetime.date.today() + datetime.timedelta(days=-2)
                if datetime.date.today() > message_date.date():
                    if message_counter < 50:
                        # позвчера
                        if message_date.date() < before_yesterday:
                            break
                        else:
                            if new_message != "":
                                telegram_list.append(
                                    [chanal_id, new_message, message_date, id_message, chanel_name])
                                message_counter += 1
                    else:
                        break
        else:
            if message_date.date() < end_date:
                break
            else:
                if new_message != "":
                    telegram_list.append(
                        [chanal_id, new_message, message_date, id_message, chanel_name])

    return telegram_list


def sorting_telegram_message(catastrophe, t_message):
    is_useful = 0
    id_mesag = None
    usefull = []
    for list_text_msg in t_message:
        for text_msg in list_text_msg:
            is_useful = 0
            if type(text_msg) == int:
                id_mesag = text_msg
            else:
                words = break_into_words(text_msg)
                for v_catastrophe in catastrophe:
                    for v_words in words:
                        if v_catastrophe == v_words:
                            is_useful = 1

        usefull.append([id_mesag, is_useful])

    return usefull


def break_into_words(text):
    wordlst = re.split(r'(?:(?!-)\W)+', text.lower())
    result = list(filter(None, wordlst))

    return result


def monitorng_message_in_database(catastrophe,bd):
    result_chek = bd.chek_sorting_telegram_message()
    if result_chek == 1:
        telegramm_mesage = bd.get_telegram_message()
        result_sorting = sorting_telegram_message(
            catastrophe, telegramm_mesage)
        if result_sorting != '':
            bd.set_sorting_msage_to_db(result_sorting)
        else:
            print("Не возможно отсортировать пустой список!")
            return 1

    return 0


def monitorng_message_in_list(catastrophe, list_message):
    if len(list_message) > 0:
        for list_text_msg in list_message:
            for text_msg in list_text_msg:
                if type(text_msg) == str:
                    words = break_into_words(text_msg)
                    for v_catastrophe in catastrophe:
                        for v_words in words:
                            if v_catastrophe == v_words:
                                list_text_msg[5] = 1
    else:
        print(" Не возможно отсортировать пустой список!")
        return 1
    return 0


def read_filter_words(file='filter-words.txt'):
    list_f_w=[]
    if file:
       f=open(file)
       for line in f:
           list_f_w.append(line.rstrip())
    else:
        print('Файл не найден! Проверьте наличие файла с данным названием.')
    return list_f_w       




def Run_bot():
    list_filter_words=[]
    list_filter_words=read_filter_words()
    # ##бд##
    db = modules.database.Database()
    # # Вставляем api_id и api_hash для клиента телеграмм
    api_id = modules.config.Api_id
    api_hash = modules.config.Api_hash
    name_sesion= "anone"
    list_filter_words=[]
    result_chek = db.chek_sorting_telegram_message()
    if result_chek == 1:
        telegramm_mesage = db.get_telegram_message()
        result_sorting = sorting_telegram_message(
            list_filter_words, telegramm_mesage)
        if result_sorting:
            db.set_sorting_msage_to_db(result_sorting)
        else:
            print("Не возможно отсортировать полученные сообщения!!!!!!!!")
    else:
        print("Введите название telegram канала: ")
        input_data_start = ''
        input_end_data = ''
        chanal_name = input()
        if chanal_name == '':
            chanal_name = modules.config.default_channel_name
        print("Вы неввели название канала, поэтому название канала= " + chanal_name)
        # ввод даты начала и окончания получения сообщения с клавиатуры. Возможно это лишняя часть
        print("Введите дату начала получения сообщения и дату последнего сообщения или нажмините 'Enter' для использования стандартных значений.")
        print("Ввод даты осуществляется в формате (год,месяц,число)\n")
        data1 = input('Дата начала диапазона: ')
        data2 = input('Дата окончания диапозона: \n')

        if data1 > data2:
            input_data_start = data1
            input_end_data = data2
        elif data1 < data2:
            input_data_start = data2
            input_end_data = data1

        if input_data_start != '':
            year, month, day = map(int, re.split(
                r'[,|\-|:.]', input_data_start))
            data_start = datetime.date(year, month, day) + \
                datetime.timedelta(days=1)
        else:
            # дата начала получения сообщений
            data_start = datetime.date.today() + datetime.timedelta(days=1)

        if input_end_data != '':
            year, month, day = map(int, re.split(
                r'[,|\-|:.]', input_end_data))
            data_experation = datetime.date(year, month, day)
        else:
            # до какой даты получать сообщения
            data_experation = input_end_data
        
        client=Create_Client_Telegram(name_sesion,api_id,api_hash)
        client.start()
        result_search_chanal_id = search_chanal_in_to_telegram(chanal_name,client)
        if result_search_chanal_id != 0:
            print("Идет получение и обработка сообщений ....")
            result_telegram_list = list_telegram_message(
                client,chanal_name, result_search_chanal_id, data_start, data_experation)
            print(result_telegram_list)
        else:
            result_telegram_list = ''
            print("Проверте название telegram канала и повторите попытку")

        if result_telegram_list != [] and result_telegram_list != '':
            db.save_result_telegram_to_db(result_telegram_list)
        else:
            if result_telegram_list == []:
                print(
                    'За введенный вами диапазон, сообщений в канале ' + '"' + chanal_name + '"' + ' не найдено или они были удалены администратором канала! Попробуйте ввести другой диапазон.')





def Comand_Run_bot(comand=''):
    
    list_filter_words=[]
    list_filter_words=read_filter_words()
    # f = open('filter-words.txt')
    # for line in f:
    #     list_filter_words.append(line.rstrip())
    api_id=comand.api_id
    api_hash=comand.api_hache
    name_sesion=comand.name_sesion
    msg_is_usseful = 0
    notdatabase = comand.nodatabase
    echo = comand.echo
    dates = ""
    db = comand.v_db
    if comand.name is not None:
        chanal_name = comand.name
    if comand.dates is not None:
        dates = comand.dates

    if len(dates) != 0:
        if len(dates) != 1:
            data1 = dates[0]
            data2 = dates[1]
        else:
            data1 = dates[0]
            data2 = ""
        if data1 > data2:
            input_data_start = data1
            input_end_data = data2
        elif data1 < data2:
            input_data_start = data2
            input_end_data = data1
    else:
        input_data_start = ""
        input_end_data = ""

    if input_data_start != '':
        year, month, day = map(int, re.split(
            r'[,|\-|:.]', input_data_start))
        data_start = datetime.date(year, month, day) + \
            datetime.timedelta(days=1)
    else:
        # дата начала получения сообщений
        data_start = datetime.date.today() + datetime.timedelta(days=1)

    if input_end_data != '':
        year, month, day = map(int, re.split(
            r'[,|\-|:.]', input_end_data))
        data_experation = datetime.date(year, month, day)
    else:
        # до какой даты получать сообщения
        data_experation = input_end_data

    client=Create_Client_Telegram(name_sesion,api_id,api_hash)
    client.start()
    result_search_chanal_id = search_chanal_in_to_telegram(chanal_name,client)
    if result_search_chanal_id != 0:
        if echo:
            print("Идет получение и обработка сообщений ....")
        result_telegram_list = list_telegram_message(
            client,chanal_name, result_search_chanal_id, data_start, data_experation)
        if echo:
            print(result_telegram_list)
    else:
        result_telegram_list = ''
        if echo:
            print("Проверте название telegram канала и повторите попытку")

    if not notdatabase:
        if db:
            bd=modules.database.Database(db)
        else:
            bd=modules.database.Database()
        if result_telegram_list != [] and result_telegram_list != '':
            result_chek = bd.chek_sorting_telegram_message()
            if result_chek == 1:
               monitorng_message_in_database(list_filter_words,bd)
            else:
              bd.save_result_telegram_to_db(result_telegram_list)
            
        else:
            if result_telegram_list == []:
                if echo:
                    print(
                        'За введенный вами диапазон, сообщений в канале ' + '"' + chanal_name + '"' + ' не найдено или они были удалены администратором канала! Попробуйте ввести другой диапазон.')
    else:
        if result_telegram_list != " ":
            for val_result_telegram in result_telegram_list:
                val_result_telegram.append(msg_is_usseful)
            result_monitorng = monitorng_message_in_list(
                list_filter_words, result_telegram_list)
            if result_monitorng == 0:
                for v_list_telegram in result_telegram_list:
                    for value in v_list_telegram:
                        if value == 1:
                            print(v_list_telegram)
        if echo:
            print(result_telegram_list)


def comand_for_bot():
    comand_for_bot = argparse.ArgumentParser(description="List of commands for controlling the bot: name_sesion,--api_id,--api_hache,--name_sesion,--date, "+
    "--echo, --name, --notdatabase,--db host port username password database.")
    comand_for_bot.add_argument(
        "name_sesion", default=None, help="Set 'name_sesion' for save sesion TelegramClient")
    comand_for_bot.add_argument(
        "--api_id", default=modules.config.Api_id, help="Set 'api_id'  telegram client after registration on https://my.telegram.org/auth")
    comand_for_bot.add_argument(
        "--api_hache", default=modules.config.Api_hash, help="Set 'api_hash'  telegram client after registration on https://my.telegram.org/auth")
    comand_for_bot.add_argument(
        "--db", dest="v_db", nargs="+", action="append", help="Set porametrs for db '--db host value --db username value --db password value'")
    comand_for_bot.add_argument(
        "--nodatabase", action="store_true", default=False, help="Bot is no save data in database set '--nodatatabase'")
    comand_for_bot.add_argument(
        "--name", type=str, default="belta_telegramm", help="Set name chanal telegram '--name string value'")
    comand_for_bot.add_argument(
        "--dates", type=str, nargs="+", help="Receiving messages for the specified period '--dates YYYY.MM.DD  YYYY.MM.DD'"
        + "example: --date 2021-11-01 2021-11-04")
    comand_for_bot.add_argument(
        "--echo", action="store_true", default=False,  help="Outputting results to the console'--echo'")

    return comand_for_bot


def comand_parse_for_bot(v_comands):
    if sys.argv[1:] != []:
       comand = v_comands.parse_args(sys.argv[1:])
    else:
        comand=None
       
    return comand    


if __name__ == "__main__":

    value_comands = comand_for_bot()
    comand= comand_parse_for_bot(value_comands)
    if comand != None:
        Comand_Run_bot(comand)
    else:
        print(" Usage arguments are required: api_id, api_hache, name_sesion in yur console")

    print("@@@ End Run in telegram_bot @@@")
