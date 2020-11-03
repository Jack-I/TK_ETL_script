import logging
from datetime import date, timedelta
from time import sleep

import schedule

from manual_load import load_one_date

logging.basicConfig(filename='TK_automatized.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filemode='w',
                    level=logging.INFO)

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)


def job():
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    load_one_date(yesterday)
    print(f"{yesterday} loading finished")


if __name__ == '__main__':
    print('Ah shit, here we go again...')
    # Schedule a job
    schedule.every().day.at("08:30").do(job)

    # Do the job
    while True:
        schedule.run_pending()
        sleep(60)  # 60 seconds
