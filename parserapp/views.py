from django.shortcuts import render
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tinkoff.invest import Client, CandleInterval
from tinkoff.invest.utils import quotation_to_decimal
from datetime import datetime, timedelta
import pytz
import time
from gspread.utils import rowcol_to_a1
import re
import traceback
from dateutil import parser as dateparser
import pandas as pd
import numpy as np
import os
import json
from django.conf import settings
from dotenv import load_dotenv

load_dotenv()

# === НАСТРОЙКИ ===

GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_PATH")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")
TOKEN = os.getenv("TINKOFF_TOKEN")
DAYS = 20
MAX_SHEET_ROWS = 1000
MAX_EXISTING_ROWS_TO_COMPARE = 2000
tz = pytz.timezone("Europe/Moscow")
MAX_RETRIES = 5
DELAY = 10

def safe_get_cell(sheet, row, col, retries=5, delay=5):
    for attempt in range(1, retries + 1):
        try:
            val = sheet.cell(row, col).value
            print(f"safe_get_cell: Получили значение с ({row},{col}): {val}")
            return val
        except Exception as e:
            print(f"safe_get_cell: Ошибка доступа к ячейке ({row},{col}), попытка {attempt}: {e}")
            time.sleep(delay)
    print(f"safe_get_cell: Не удалось получить значение с ({row},{col}) после {retries} попыток")
    return None

def parse_range_bounds(cell_range):
    match = re.search(r'([A-Z]+)(\d+):[A-Z]+(\d+)', cell_range)
    if match:
        start_row = int(match.group(2))
        end_row = int(match.group(3))
        print(f"parse_range_bounds: Диапазон строк: {start_row} - {end_row}")
        return (start_row, end_row)
    print(f"parse_range_bounds: Не удалось распарсить диапазон: {cell_range}")
    return (None, None)

def ensure_enough_rows(sheet, required_row):
    current_rows = len(sheet.get_all_values())
    if required_row > current_rows:
        print(f"ensure_enough_rows: Добавляем {required_row - current_rows} строк до {required_row}")
        sheet.add_rows(required_row - current_rows)
    else:
        print(f"ensure_enough_rows: Строк достаточно ({current_rows} >= {required_row})")

def run_parser():
    output = []
    try:
        print("run_parser: Начинаем авторизацию в Google Sheets...")
        output.append("Начинаем авторизацию в Google Sheets...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
        client_gs = gspread.authorize(creds)
        sheet = client_gs.open(SPREADSHEET_NAME).sheet1
        output.append("Успешно авторизованы и получили доступ к таблице")

        tickers_row = 4
        figi_row = 5
        start_col = 3
        num_tickers = 41

        tickers, figis, cols_for_data = [], [], []

        print(f"run_parser: Получаем список из {num_tickers} тикеров и FIGI...")
        output.append(f"Получаем список из {num_tickers} тикеров и FIGI...")

        for i in range(num_tickers):
            col = start_col + i * 2
            ticker = safe_get_cell(sheet, tickers_row, col)
            time.sleep(0.6)
            figi = safe_get_cell(sheet, figi_row, col)
            time.sleep(0.6)
            if ticker and figi:
                tickers.append(ticker.strip())
                figis.append(figi.strip())
                cols_for_data.append((col, col + 1))

        output.append(f"Найдено тикеров: {len(tickers)}")
        print(f"run_parser: Найдено тикеров: {len(tickers)}")

        with Client(TOKEN) as client_invest:
            for ticker, figi, (date_col, price_col) in zip(tickers, figis, cols_for_data):
                output.append(f"⏳ Загрузка данных для {ticker} (FIGI: {figi})")
                print(f"run_parser: Начинаем загрузку данных для {ticker} (FIGI: {figi})")

                candles_data = []

                all_dates = sheet.col_values(date_col)[7:]  # пропускаем заголовки
                print(f"run_parser: В таблице для {ticker} найдено {len(all_dates)} записей дат")

                last_datetime = None
                if all_dates:
                    try:
                        parsed_datetimes = [dateparser.parse(d) for d in all_dates if d]
                        if parsed_datetimes:
                            last_datetime = max(parsed_datetimes)
                        print(f"run_parser: Последний datetime в таблице для {ticker}: {last_datetime}")
                    except Exception as e:
                        print(f"run_parser: Ошибка парсинга даты для {ticker}: {e}")

                now = datetime.now(tz)

                if last_datetime is None:
                    start_date = now - timedelta(days=DAYS)
                    start_from = tz.localize(datetime(start_date.year, start_date.month, start_date.day, 10, 0))
                else:
                    # Начинаем с последнего сохранённого времени + 5 минут
                    start_from = last_datetime + timedelta(minutes=5)
                    if start_from.tzinfo is None:
                        start_from = tz.localize(start_from)

                current_dt = start_from

                while current_dt < now:
                    day_start = tz.localize(datetime(current_dt.year, current_dt.month, current_dt.day, 10, 0))
                    day_end = tz.localize(datetime(current_dt.year, current_dt.month, current_dt.day, 18, 45))

                    query_from = current_dt if current_dt.date() == start_from.date() else day_start
                    query_to = day_end

                    try:
                        candles = client_invest.market_data.get_candles(
                            figi=figi,
                            from_=query_from,
                            to=query_to,
                            interval=CandleInterval.CANDLE_INTERVAL_5_MIN
                        ).candles

                        for candle in candles:
                            candle_time = candle.time.astimezone(tz)
                            close_price = float(quotation_to_decimal(candle.close))
                            candles_data.append([candle_time.strftime('%Y-%m-%d %H:%M:%S'), close_price])

                        print(f"run_parser: Получено {len(candles)} свечей для {ticker} за {current_dt.date()}")
                        time.sleep(0.15)
                    except Exception as e:
                        err_msg = f"❌ Ошибка {ticker} {current_dt.date()}: {e}"
                        output.append(err_msg)
                        print(err_msg)

                    current_dt = day_start + timedelta(days=1)

                candles_data.sort(key=lambda x: x[0])

                last_dates = all_dates[-MAX_EXISTING_ROWS_TO_COMPARE:]
                existing_dates = set(last_dates)
                new_candles = [row for row in candles_data if row[0] not in existing_dates]

                if not new_candles:
                    output.append(f"✅ Нет новых данных для {ticker}")
                    print(f"run_parser: Нет новых данных для {ticker}")
                    continue

                first_free_row = len(all_dates) + 8
                dates = [[row[0]] for row in new_candles]
                prices = [[row[1]] for row in new_candles]

                date_range = f"{rowcol_to_a1(first_free_row, date_col)}:{rowcol_to_a1(first_free_row + len(dates) - 1, date_col)}"
                price_range = f"{rowcol_to_a1(first_free_row, price_col)}:{rowcol_to_a1(first_free_row + len(prices) - 1, price_col)}"

                _, date_end_row = parse_range_bounds(date_range)
                if date_end_row:
                    ensure_enough_rows(sheet, date_end_row)

                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        sheet.update(range_name=date_range, values=dates)
                        sheet.update(range_name=price_range, values=prices)
                        output.append(f"✅ Добавлено {len(new_candles)} свечей для {ticker}")
                        print(f"run_parser: Успешно записано {len(new_candles)} новых свечей для {ticker}")
                        break
                    except Exception as e:
                        err_msg = f"❌ Ошибка записи {ticker}, попытка {attempt}: {e}"
                        output.append(err_msg)
                        print(err_msg)
                        time.sleep(DELAY)
                else:
                    output.append(f"🚫 Не удалось записать данные для {ticker}")
                    print(f"run_parser: Не удалось записать данные для {ticker} после {MAX_RETRIES} попыток")

    except Exception as e:
        err_trace = traceback.format_exc()
        output.append("❌ Общая ошибка:\n" + err_trace)
        print(f"run_parser: Общая ошибка:\n{err_trace}")

    return "\n".join(output)
def home(request):
    message = ""
    correlation_data = []
    growth_data = []

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Получен {request.method} запрос на /home/")

    if request.method == 'POST':
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ▶️ Запускаем парсер...")
        message = run_parser()  # твоя функция парсинга, возвращает строку
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Парсер завершен.")
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] GET запрос — парсер не запускается.")

    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Загружаем корреляции
    correlation_file = os.path.join(current_dir, 'correlations.json')
    if os.path.exists(correlation_file):
        try:
            with open(correlation_file, 'r', encoding='utf-8') as f:
                all_correlations = json.load(f)
            filtered = [c for c in all_correlations if c.get('correlation') is not None]
            sorted_corr = sorted(filtered, key=lambda x: abs(x['correlation']), reverse=True)
            correlation_data = sorted_corr[:10]
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Загружено {len(correlation_data)} топ корреляций.")
        except Exception as e:
            print(f"⚠️ Ошибка при чтении correlations.json: {e}")
    else:
        print(f"⚠️ Файл correlations.json не найден по пути: {correlation_file}")

    # Загружаем данные по росту/падению
    growth_file = os.path.join(current_dir, 'growth.json')
    if os.path.exists(growth_file):
        try:
            with open(growth_file, 'r', encoding='utf-8') as f:
                growth_all = json.load(f)

            # Фильтруем только с числовым ростом
            growth_all = [g for g in growth_all if g.get('growth_percent') is not None]

            # Лидеры роста — top 5 по убыванию роста (>0)
            leaders_up = sorted(
                [g for g in growth_all if g['growth_percent'] > 0],
                key=lambda x: x['growth_percent'],
                reverse=True
            )[:5]

            # Лидеры падения — top 5 по возрастанию роста (<0)
            leaders_down = sorted(
                [g for g in growth_all if g['growth_percent'] < 0],
                key=lambda x: x['growth_percent']
            )[:5]

            growth_data = {
                'leaders_up': leaders_up,
                'leaders_down': leaders_down
            }

            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Загружено лидеров роста и падения.")
        except Exception as e:
            print(f"⚠️ Ошибка при чтении growth.json: {e}")
    else:
        print(f"⚠️ Файл growth.json не найден по пути: {growth_file}")

    return render(request, 'parserapp/home.html', {
        'message': message,
        'top_correlations': correlation_data,
        'growth_data': growth_data,
    })

def get_prices_df(sheet, date_col, price_col):
    """
    Получаем DataFrame с датами (index) и ценами из листа Google Sheets
    """
    dates = sheet.col_values(date_col)[7:]  # пропускаем заголовки
    prices = sheet.col_values(price_col)[7:]
    data = []
    for d, p in zip(dates, prices):
        if d and p:
            try:
                dt = dateparser.parse(d)
                price = float(p)
                data.append((dt, price))
            except Exception:
                continue
    df = pd.DataFrame(data, columns=['datetime', 'price'])
    df.set_index('datetime', inplace=True)
    return df.sort_index()

def find_ticker_columns(sheet, ticker):
    """
    Находит столбцы с датами и ценами по тикеру
    Возвращает (date_col, price_col) или (None, None)
    """
    tickers_row = 4
    start_col = 3
    num_tickers = 41
    for i in range(num_tickers):
        col = start_col + i * 2
        cell = safe_get_cell(sheet, tickers_row, col)
        if cell and cell.strip().lower() == ticker.strip().lower():
            return (col, col + 1)
    return (None, None)


# Правильно дает данные последних 1000 строк
def correlation_view(request):
    message = ""
    correlation = None 
    if request.method == 'POST':
        ticker1 = request.POST.get('ticker1', '').strip().upper()
        ticker2 = request.POST.get('ticker2', '').strip().upper()

        if ticker1 and ticker2:
            try:
                # Авторизация Google Sheets
                scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
                creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
                client_gs = gspread.authorize(creds)
                sheet = client_gs.open(SPREADSHEET_NAME).sheet1

                # Поиск столбцов для тикера 1
                date_col1, price_col1 = find_ticker_columns(sheet, ticker1)
                # Поиск столбцов для тикера 2
                date_col2, price_col2 = find_ticker_columns(sheet, ticker2)

                if None in (date_col1, price_col1):
                    message = f"Тикер {ticker1} не найден."
                elif None in (date_col2, price_col2):
                    message = f"Тикер {ticker2} не найден."
                else:
                    values = sheet.get_all_values()
                    rows = values[7:]  # Пропускаем заголовки

                    # Получаем данные по первому тикеру
                    data1 = []
                    for row in rows:
                        if len(row) <= price_col1:
                            continue
                        d1 = row[date_col1].strip()
                        p1 = row[price_col1].strip()
                        if d1 and p1:
                            data1.append((d1, p1))
                    last_data1 = data1[-1000:]

                    # Получаем данные по второму тикеру
                    data2 = []
                    for row in rows:
                        if len(row) <= price_col2:
                            continue
                        d2 = row[date_col2].strip()
                        p2 = row[price_col2].strip()
                        if d2 and p2:
                            data2.append((d2, p2))
                    last_data2 = data2[-1000:]

                    print(f"\n===== Последние {len(last_data1)} строк по {ticker1} =====")
                    for record in last_data1:
                        print(record)
                    print(f"===== Конец данных {ticker1} =====\n")

                    print(f"\n===== Последние {len(last_data2)} строк по {ticker2} =====")
                    for record in last_data2:
                        print(record)
                    print(f"===== Конец данных {ticker2} =====\n")

                    data1, data2 = [], []
                    for record in last_data1:
                        data1.append(record[0])
                    for record in last_data2:
                        data2.append(record[0])
                    # Функция для преобразования списка строк в список float
                    def convert_to_float(lst):
                        return [float(x.replace(',', '.')) for x in lst]
                    # Конвертация списков
                    arr1 = convert_to_float(data1)
                    arr2 = convert_to_float(data2)

                    # Вычисление корреляции Пирсона
                    correlation = np.corrcoef(arr1, arr2)[0, 1]

                    print(f"Корреляция: {correlation}")

                    message = f"Выведены последние строки по {ticker1} и {ticker2} в консоль."

            except Exception as e:
                message = f"Ошибка: {e}"
        else:
            message = "Пожалуйста, введите оба тикера."

    return render(request, 'parserapp/correlation.html', {'message': message, 'correlation': correlation})
