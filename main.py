import modules.telegram_bot 

if __name__=="__main__":
   cd=modules.telegram_bot.comand_for_bot()
   result=modules.telegram_bot.comand_parse_for_bot(cd)

   if result:
      modules.telegram_bot.Comand_Run_bot(result)
   else:
     print(" Usage arguments are required: api_id, api_hache, name_sesion in yur console")


