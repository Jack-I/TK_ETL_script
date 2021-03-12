import logging
from datetime import datetime, timedelta, timezone
from time import sleep

import schedule
from telegram.ext import CommandHandler
from telegram.ext import Updater

import bot_functions as b_f
from manual_load import load_one_date
from secrets import bot_token

logging.basicConfig(filename='TK_automatized.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filemode='w',
                    level=logging.INFO)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

# bot stuff
updater = Updater(token=bot_token, use_context=True)
dispatcher = updater.dispatcher
start_handler = CommandHandler('start', b_f.start)
dispatcher.add_handler(start_handler)
dispatcher.add_error_handler(b_f.error)
updater.start_polling()
logging.info("BOT DEPLOYED")


def job():
    # Yesterday in SPb time (UTC +3)
    yesterday = (datetime.now(timezone(timedelta(hours=+3)))
                         - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        load_one_date(yesterday)
    except Exception as e:
        updater.bot.send_message(chat_id='-479705730', text=f'Скрипт упал с ошибкой {e}')
        print('Error:', e)
    else:
        # Ok-message disabled (commented) for peace of mind
        # updater.bot.send_message(chat_id='-479705730', text='Выгрузки произведены')
        print(f"{yesterday} loading finished")


if __name__ == '__main__':
    print('Ah shit, here we go again...')
    # Schedule a job
    schedule.every().day.at("05:25").do(job)  # +3 = 8:25 at SPb. UTC time dirty hack

    # Do the job
    while True:
        schedule.run_pending()
        sleep(60)  # 60 seconds
