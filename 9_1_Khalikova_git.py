import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import datetime, timedelta
import io
from airflow.decorators import dag, task


# Подключение к ClickHouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20250520'
}

# Telegram параметры
bot_token = '___'
chat_id = -1002614297220  # 1023943467
bot = telegram.Bot(token=bot_token)

# Параметры DAG
default_args = {
    'owner': 'aj-halikova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 27)
}
schedule_interval = '*/15 * * * *'

# Конфигурация метрик
METRICS_CONFIG = {
    'users_feed': {
        'name': 'Активные пользователи в ленте',
        'group': 'feed_actions',
        'iqr_a': 3,  # коэффициент для межквартильного размаха
        'iqr_n': 6,  # размер окна для IQR
        'sigma_a': 3,  # коэффициент для правила сигм
        'sigma_n': 8,  # размер окна для правила сигм
        'day_threshold': 0.25,  # порог отклонения от вчера
    },
    'users_message': {
        'name': 'Активные пользователи в мессенджере',
        'group': 'message_actions',
        'iqr_a': 3,
        'iqr_n': 6,
        'sigma_a': 3,
        'sigma_n': 8,
        'day_threshold': 0.25,
    },
    'views': {
        'name': 'Просмотры',
        'group': 'feed_actions',
        'iqr_a': 3,
        'iqr_n': 6,
        'sigma_a': 3,
        'sigma_n': 6,
        'day_threshold': 0.3,
    },
    'likes': {
        'name': 'Лайки',
        'group': 'feed_actions',
        'iqr_a': 3,
        'iqr_n': 6,
        'sigma_a': 3,
        'sigma_n': 6,
        'day_threshold': 0.3,
    },
    'ctr': {
        'name': 'CTR',
        'group': 'feed_actions',
        'iqr_a': 3,
        'iqr_n': 6,
        'sigma_a': 3,
        'sigma_n': 8,
        'day_threshold': 0.4,
    },
    'messages': {
        'name': 'Отправленные сообщения',
        'group': 'message_actions',
        'iqr_a': 2.5,
        'iqr_n': 8,
        'sigma_a': 3,
        'sigma_n': 10,
        'day_threshold': 0.3,
    }
}

def check_anomaly_iqr(df, metric, a=3, n=5): # методом межквартильного размаха
    df = df.copy()
    
    df['q25'] = df[metric].shift(1).rolling(n, center=True, min_periods=1).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n, center=True, min_periods=1).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    
    df['up_iqr'] = df['q75'] + a * df['iqr']
    df['low_iqr'] = df['q25'] - a * df['iqr']
    
    df['up_iqr'] = df['up_iqr'].rolling(n, center=True, min_periods=1).mean()
    df['low_iqr'] = df['low_iqr'].rolling(n, center=True, min_periods=1).mean()
    
    current_value = df[metric].iloc[-1]
    if current_value < df['low_iqr'].iloc[-1] or current_value > df['up_iqr'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

def check_anomaly_sigma(df, metric, a=3, n=5): # правило трех сигм
    df = df.copy()
    
    df['mean'] = df[metric].shift(1).rolling(n, center=True, min_periods=1).mean()
    df['std'] = df[metric].shift(1).rolling(n, center=True, min_periods=1).std()
    
    df['up_sigma'] = df['mean'] + a * df['std']
    df['low_sigma'] = df['mean'] - a * df['std']

    df['up_sigma'] = df['up_sigma'].rolling(n, center=True, min_periods=1).mean()
    df['low_sigma'] = df['low_sigma'].rolling(n, center=True, min_periods=1).mean()
    
    current_value = df[metric].iloc[-1]
    if current_value < df['low_sigma'].iloc[-1] or current_value > df['up_sigma'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

def check_anomaly_day_ago(df, metric, threshold=0.3): # Сравнение с аналогичной 15-минуткой день назад
    current_ts = df['ts'].max()
    day_ago_ts = current_ts - pd.DateOffset(days=1)
    
    try:
        current_value = df[df['ts'] == current_ts][metric].iloc[0]
        day_ago_data = df[df['ts'] == day_ago_ts]
        
        if len(day_ago_data) == 0:
            return 0, current_value, 0
            
        day_ago_value = day_ago_data[metric].iloc[0]
        
        if day_ago_value == 0:
            return 0, current_value, 0
        
        if current_value <= day_ago_value:
            diff = abs(current_value / day_ago_value - 1)
        else:
            diff = abs(day_ago_value / current_value - 1)
        
        is_alert = int(diff > threshold)
        return is_alert, current_value, diff
    except Exception:
        return 0, 0, 0

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_ajhalikova():
    
    @task()
    def extract_data():
        query = '''
        WITH feed AS (
            SELECT toStartOfFifteenMinutes(time) as ts,
                   toDate(ts) as date,
                   formatDateTime(ts, '%R') as hm,
                   uniqExact(user_id) as users_feed,
                   countIf(user_id, action = 'view') as views,
                   countIf(user_id, action = 'like') as likes,
                   if(views > 0, likes / views, 0) as ctr
            FROM simulator_20250520.feed_actions
            WHERE ts >= today() - 1 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
        ),
        msg AS (
            SELECT toStartOfFifteenMinutes(time) as ts,
                   toDate(ts) as date,
                   formatDateTime(ts, '%R') as hm,
                   uniqExact(user_id) as users_message,
                   count(user_id) as messages
            FROM simulator_20250520.message_actions
            WHERE ts >= today() - 1 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
        )
        SELECT feed.ts, feed.date, feed.hm, users_feed, views, likes, ctr, users_message, messages
        FROM feed
        FULL OUTER JOIN msg ON feed.ts = msg.ts
        ORDER BY ts
        '''
        
        df = ph.read_clickhouse(query, connection=connection)
        df = df.fillna(0)
        
        int_metrics = ['users_feed', 'views', 'likes', 'users_message', 'messages']
        df[int_metrics] = df[int_metrics].astype('int64')
        
        return df

    @task()
    def run_alerts(df): # Проверка метрик на аномалии и отправка алертов, использование ансамбля методов для повышения точности
        
        # Проверяем, что у нас достаточно данных для анализа
        if len(df) < 2:
            print("Недостаточно данных для анализа аномалий")
            return
                
        for metric, config in METRICS_CONFIG.items():
            df_metric = df[['ts', 'date', 'hm', metric]].copy()
            
            # Пропускаем метрики с нулевыми значениями
            if df_metric[metric].sum() == 0:
                continue
            
            # Проверка аномалий тремя методами
            is_alert_iqr, df_metric_iqr = check_anomaly_iqr(
                df_metric, metric, 
                a=config['iqr_a'], 
                n=config['iqr_n']
            )
            is_alert_sigma, df_metric_sigma = check_anomaly_sigma(
                df_metric, metric,
                a=config['sigma_a'],
                n=config['sigma_n']
            )
            is_alert_day, current_val, diff_day = check_anomaly_day_ago(
                df_metric, metric, 
                threshold=config['day_threshold']
            )
            
            # Принятие решения об аномалии по принципу "хотя бы один метод"
            if is_alert_iqr or is_alert_sigma or is_alert_day:
                current_value = df_metric[metric].iloc[-1]
                prev_value = df_metric[metric].iloc[-2] if len(df_metric) > 1 else 0
                
                if prev_value > 0:
                    deviation_prev = abs(1 - (current_value / prev_value))
                else:
                    deviation_prev = 0
                
                max_deviation = max(deviation_prev, diff_day)
                
                if metric == 'ctr':
                    current_value_str = f"{current_value:.4f}"
                    max_deviation_str = f"{max_deviation:.2%}"
                else:
                    current_value_str = f"{current_value:.0f}"
                    max_deviation_str = f"{max_deviation:.2%}"
                
                msg = (
                    f"🚨 <b>Аномалия в метрике {config['name']} в срезе {config['group']}</b>\n\n"
                    f"📊 <b>Текущее значение:</b> {current_value_str}\n"
                    f"📈 <b>Отклонение более:</b> {max_deviation_str}\n\n"
                )
                
                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()
                
                fig, ax = plt.subplots(figsize=(16, 10))
                
                ax.plot(df_metric['ts'], df_metric[metric], label='Метрика', linewidth=2, color='blue')
                
                # Границы IQR метода
                ax.plot(df_metric['ts'], df_metric_iqr['up_iqr'], label='IQR верхняя граница', 
                       linestyle='--', color='red', alpha=0.7)
                ax.plot(df_metric['ts'], df_metric_iqr['low_iqr'], label='IQR нижняя граница', 
                       linestyle='--', color='red', alpha=0.7)
                
                # Границы правила сигм
                ax.plot(df_metric['ts'], df_metric_sigma['up_sigma'], label='Правило сигм верхняя граница', 
                       linestyle=':', color='orange', alpha=0.7)
                ax.plot(df_metric['ts'], df_metric_sigma['low_sigma'], label='Правило сигм нижняя граница', 
                       linestyle=':', color='orange', alpha=0.7)
                
                # Выделение аномальных точек
                anomalies_iqr = (df_metric[metric] > df_metric_iqr['up_iqr']) | (df_metric[metric] < df_metric_iqr['low_iqr'])
                anomalies_sigma = (df_metric[metric] > df_metric_sigma['up_sigma']) | (df_metric[metric] < df_metric_sigma['low_sigma'])
                
                if anomalies_iqr.any():
                    anomalies_iqr_points = df_metric[anomalies_iqr]
                    ax.scatter(anomalies_iqr_points['ts'], anomalies_iqr_points[metric], 
                             color='red', s=100, label='IQR аномалии', zorder=5)
                
                if anomalies_sigma.any():
                    anomalies_sigma_points = df_metric[anomalies_sigma]
                    ax.scatter(anomalies_sigma_points['ts'], anomalies_sigma_points[metric], 
                             color='orange', s=80, label='Правило сигм аномалии', zorder=5)
                
                # Особое выделение текущей аномальной точки
                if is_alert_iqr or is_alert_sigma:
                    current_point = df_metric.iloc[-1]
                    ax.scatter(current_point['ts'], current_point[metric], color='darkred', 
                             s=150, marker='X', label='Текущая аномалия', zorder=6)
                
                # Настройка графика
                ax.set_xlabel('Время', fontsize=12)
                ax.set_ylabel(config['name'], fontsize=12)
                ax.set_title(f'Аномалия: {config["name"]} (IQR + Правило сигм)', fontsize=14, fontweight='bold')
                ax.grid(True, alpha=0.3)
                ax.legend()
                
                # Разряжение подписей по оси X
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 4 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                
                plt.xticks(rotation=45, ha='right')
                ax.set_ylim(bottom=0)
                
                # Сохранение и отправка графика
                plot_object = io.BytesIO()
                plt.savefig(plot_object, bbox_inches='tight', dpi=300)
                plot_object.seek(0)
                plot_object.name = f'{metric}_anomaly.png'
                plt.close()
                
                # Отправка алерта
                bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=msg, parse_mode='HTML')
    
    df = extract_data()
    run_alerts(df)

alert_ajhalikova = alert_ajhalikova() 