import pandas as pd
import numpy as np
import pandahouse
import seaborn as sns
import matplotlib.pyplot as plt
import os
import datetime
from datetime import timedelta
import time

import telegram
import io

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'mih-m',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime.datetime(2023, 9, 17),}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def Alert_mih_m_result():
    # Настройка бота
    my_token = '' # тут нужно заменить на токен вашего бота
    bot = telegram.Bot(token=my_token)
    chat_id = 000000
    chat_id_my = 111111
    
    # Словарь для осмысленной расшифровки фичей
    transcript = {
    'feed_au': 'AU (акт.п.) стена, все',
    'ios_feed_au': 'AU (акт.п.) стена, IOS',
    'android_feed_au': 'AU (акт. п.) стена, Android',
    'views': 'Просм. постов стены, все',
    'ios_views': 'Просм. постов стены, IOS',
    'android_views': 'Просм. постов стены, Android',
    'likes': 'Лайки постов стены, все',
    'ios_likes': 'Лайки постов стены, IOS',
    'android_likes': 'Лайки постов стены, Android',
    'CTR': 'CTR (likes/views) стены',
    'mess_active_users': 'AU (акт.п.) месс, все',
    'ios_mess_au': 'AU (акт.п.) месс, IOS',
    'android_mess_au': 'AU (акт.п.) месс, Android',
    'messages': 'Сообщений месс, все'}
    
    @task
    def get_data():
        # Ходит в кликхаус за данными
        
        connection = {
            'host': 'https://clickhouse',
            'password': 'pass',
            'user': 'user',
            'database': 'database'}

        query = '''
    WITH feed as (
    SELECT
        toStartOfFifteenMinutes(time) + INTERVAL 15 MINUTE as timespan,

        COUNT(distinct user_id) as feed_au,
        countIf(distinct user_id, os = 'iOS') as ios_feed_au,
        countIf(distinct user_id, os != 'iOS') as android_feed_au,

        countIf(action, action='view') as views,
        countIf(action, action='view' and os = 'iOS') as ios_views,
        countIf(action, action='view' and os != 'iOS') as android_views,

        countIf(action, action='like') as likes,
        countIf(action, action='like' and os = 'iOS') as ios_likes,
        countIf(action, action='like' and os != 'iOS') as android_likes,

        countIf(action, action='like') * 100 / countIf(action, action='view') as CTR
    FROM simulator_20230820.feed_actions
    WHERE 
    time >= toStartOfFifteenMinutes(now()) - INTERVAL 240 MINUTE
    and
    time < toStartOfFifteenMinutes(now())
    GROUP BY
        toStartOfFifteenMinutes(time)
    ORDER BY
        timespan 
    ),
    mess as (
    SELECT
        toStartOfFifteenMinutes(time) + INTERVAL 15 MINUTE as timespan,
        COUNT(distinct user_id) as mess_active_users,
        countIf(distinct user_id, os = 'iOS') as ios_mess_au,
        countIf(distinct user_id, os != 'iOS') as android_mess_au,
        count(reciever_id) as messages
    FROM simulator_20230820.message_actions
    WHERE 
    time >= toStartOfFifteenMinutes(now()) - INTERVAL 240 MINUTE
    and
    time < toStartOfFifteenMinutes(now())
    GROUP BY
        toStartOfFifteenMinutes(time)
    ORDER BY
        timespan 
    )

    SELECT *
    FROM feed
    JOIN mess
    USING(timespan)   
    '''
        return pandahouse.read_clickhouse(query, connection=connection)
    
    def my_mape(df):
        # Функция оцениваетп роцентное отклонение точки от скользящей средней 3х прошлых точек для интервалов 15мин
        x = 45
        y1 = df.iloc[:2].mean()
        #print(y1)
        x1 = 7.5
        y2 = df.iloc[1:3].mean()
        #print(y2)
        x2 = 7.5 + 15
        y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)
        f = df.iloc[3]
        return round(f, 2), round(y, 2), round((f - y) * 100 / abs(f), 2)
    
    @task
    def detect_anomaly(df:pd.DataFrame, threshold:int = 55):
        '''
        Фукция детектирует превышение порога {tr}% процентной ошибки последней точки, от скользящей средней по 3 точкам до.
        Далее, если в 3 точках до выброса есть также превышение этого порога ошибки,
        алгоритм считает сигнал по последнему измерению
        следствием выброса в исторических данных и игнорирует его.
        Если этот фактор не учитывать, система задетектит 1ый выброс, но также будет детектировать 
        аномалии еще 2-3 периода после
        из-за колебания скользящего среднего.
        То есть система детектирует 1ый выброс, после чего становится нечувствительной на следующие 3 периода.
        '''
        
        # В ночное время (2-10 ночи) будем уменьшать чувствительность чтобы увеличивать допустимые интервалы колебаний.
        check_time = (max(df['timespan']).hour > 9)|(max(df['timespan']).hour < 2)
        
        # Для точной подстройки чувствительности None = значение по умолчанию
        '''
        Конкретные значения чувствительности подобрал отдельно для каждой фичи
        Подробнее тут: 
        https://git.lab.karpov.courses/practice-da/airflow-alerts/-/blob/master/dags/mih-m/intervals.ipynb
        '''
        thresholds = {
            # feed_au 35/70% день/ночь
            'feed_au': 35,
            # ios_feed_au 35/70% день/ночь
            'ios_feed_au': 35,
            # android_feed_au 35/70% день/ночь
            'android_feed_au': 35,
            # views likes 60/120% день/ночь
            'views': 60,
            'ios_views': 60,
            'android_views': 60,
            'likes': 60,
            'ios_likes': 60,
            'android_likes': 60,
            #Для пользователей стены большие интервалы т.к. пользователей мало и ночью большая дисперсия значений
            'mess_active_users': 140,
            'ios_mess_au': 160,
            'android_mess_au': 160,
            'messages': 170,
            # CTR как метрика отношений очень стабильная
            'CTR': 20
            }
        
        # Отчет о проверке
        report = {}

        # Флаг для логирования
        flag = False
        
        for col in df.drop(columns='timespan'):
            # Подсчет скользящего среднего для всех точек выгруженного интервала
            check = [my_mape(df[col][x:x + 4]) for x in range(0, len(df[col]) - 3)]
            
            # Если порог задан в словаре точной подстройки - используем его, иначе значение по умолчанию
            thr = thresholds[col] if thresholds[col] else threshold
            
            # Если дневное время - используем порог чувствительности thr, если ночное то увеичиваем порог в N раз
            thr = thr if check_time else thr * 2
            
            if abs(check[-1][-1]) > thr:
                flag = True
                if any([abs(x[-1]) > thr for x in check[-3: -1]]):
                    flag = False
                    print(f'\nЛожное срабатывание по {col}')
                else:
                    print('>>> Обнаружена аномалия <<<')
                    report[col] = check[-1]
                    print(col, 'значение', check[-1][0],'Прогноз', check[-1][1], 'отклонение', f'{round(check[-1][-1],2)}%')
            else:
                print(f'Метрика:{col} -> OK {round(check[-1][-1], 2)}%')
        print('>>>>>>>> Сработал алерт <<<<<<<<<' if report else 'Все ОК, и Вы прекрасны')
        return report
    
    @task
    def transform_text_report(report:dict):
        # Создает текстовый репорт в чат если система алертов сработала

        # Заголовок
        text_report = [f'🔴 Система оповещения 🔴']

        # Перебираем все фичи из репорта
        if report:
            for metric in report:
                text_report.append(f'Метрика:\n❗{transcript[metric]}❗')
                text_report.append(f'Текущее значение {report[metric][0]}. Отклонение {report[metric][2]}%\n')

            # Добавляем в конец ссылки и штеги
            text_report.append(f'Дашборд мониторинга:\nhttps://superset.lab.karpov.courses/superset/dashboard/4260/')
            text_report.append(f'@maryginm, требуется анализ ситуации')           
        else:
            return None

        # Возрващаем текстовый репорт
        return '\n'.join(text_report)
    
    @task
    def transform_graph_report(report:dict, df:pd.DataFrame):
        # Обработка для графиков
        if report:
            # Создаем ячейки субплота в зависимиости от количества метрик
            fig, axes = plt.subplots(len(report), 1, sharex=True, figsize=(10, 5*len(report)))
            
            # Счетчик по ключам репорта
            counter = 0
            names = tuple(report.keys())
            
            # Трюк с условием, для того, чтобы можно было строить и 1 и несколько графиков через субплот
            for el in axes if len(report) > 1 else [axes]:
                metric = names[counter]
                
                # Даты на всех графиках
                el.tick_params(axis='x', labelbottom=True)

                # цвет фона
                el.set(facecolor="xkcd:charcoal")

                # включить второстепенные деления осей:
                el.minorticks_on()

                # вид линий основной сетки:
                el.grid(which='major',
                    color='white',
                    linewidth=0.3)

                # вид линий вспомогательной сетки:
                el.grid(which='minor',
                        color='white',
                        linewidth=0.1,
                        linestyle=':')

                # Подсчет данных для графика
                res_pred = [round(my_mape(df[metric][x:x + 4])[1]) for x in range(0, len(df[metric]) - 3)]
                
                el.set_title(metric)
                el.set_ylabel(f'{transcript[metric]}')
                el.set_xlabel(f'Интервалы 15 мин.')
                
                # График метрики
                sns.lineplot(ax=el, x=df.timespan,
                             y=df[metric], label=f'{transcript[metric]}',
                             marker='o', linestyle='-', linewidth=2)
                
                # График прогноза по ск. среднему
                sns.lineplot(ax=el, x=df.timespan[3:],
                             y=res_pred, label='Ожидаемое значение',
                             marker='x', linestyle=':', linewidth=3)
                el.legend()

                counter += 1

            plot_object = io.BytesIO()
            fig.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'alert_plot.png'
        else:
            return None
        
        return plot_object
    
    @task
    def send_report(text_report, plot_object):
        if text_report:
            # Отправка текстового отчета
            bot.sendMessage(chat_id=chat_id, text=text_report)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            plot_object.seek(0)
            bot.sendMessage(chat_id=chat_id_my, text=text_report)
            bot.sendPhoto(chat_id=chat_id_my, photo=plot_object)
            
    # Запускаем шарманку
    
    # Грузим данные
    dfr = get_data()
    
    # Запускаем анализ аномалий
    report = detect_anomaly(dfr)
    
    # Отправляем отчет
    send_report(transform_text_report(report), transform_graph_report(report, dfr))

    
dag_name = Alert_mih_m_result()
