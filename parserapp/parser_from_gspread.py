import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tinkoff.invest import Client, CandleInterval
from tinkoff.invest.utils import quotation_to_decimal
from datetime import datetime, timedelta
import pytz
import time
from gspread.utils import rowcol_to_a1
import re

# === НАСТРОЙКИ ===
GOOGLE_CREDENTIALS_FILE = "google_sheets_api.json"
SPREADSHEET_NAME = "Спредовая торговля"
TOKEN = "t.DdSB2WN4petW-XNHWszOo0MoW41C-Eo6v-z9T6Mx5vptze2B7NbtFQkW6Yr9iPOYs5KasDaoQAYOFdAdi67jEw"
DAYS = 20  # Кол-во дней для загрузки
MAX_SHEET_ROWS = 1000
MAX_EXISTING_ROWS_TO_COMPARE = 2000  # Только последние 2000 строк для проверки на дубликаты
tz = pytz.timezone("Europe/Moscow")
MAX_RETRIES = 5
DELAY = 10

def safe_get_cell(sheet, row, col, retries=5, delay=5):
    for _ in range(retries):
        try:
            return sheet.cell(row, col).value
        except Exception as e:
            print(f"Ошибка чтения ячейки ({row}, {col}): {e}. Повтор через {delay} секунд...")
            time.sleep(delay)
    print(f"Не удалось прочитать ячейку ({row}, {col}) после {retries} попыток")
    return None

def parse_range_bounds(cell_range):
    match = re.search(r'([A-Z]+)(\d+):[A-Z]+(\d+)', cell_range)
    return (int(match.group(2)), int(match.group(3))) if match else (None, None)

def ensure_enough_rows(sheet, required_row):
    current_rows = len(sheet.get_all_values())
    if required_row > current_rows:
        rows_to_add = required_row - current_rows
        print(f"📈 Добавляем {rows_to_add} строк (текущие: {current_rows}, нужно: {required_row})")
        sheet.add_rows(rows_to_add)

def main():
    # Авторизация
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
    client_gs = gspread.authorize(creds)
    sheet = client_gs.open(SPREADSHEET_NAME).sheet1

    tickers_row = 4
    figi_row = 5
    start_col = 3
    num_tickers = 41

    tickers, figis, cols_for_data = [], [], []

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

    print(f"Найдено тикеров: {len(tickers)}")

    with Client(TOKEN) as client_invest:
        for ticker, figi, (date_col, price_col) in zip(tickers, figis, cols_for_data):
            print(f"\n🔄 Загрузка данных для {ticker} (FIGI: {figi})")
            candles_data = []

            now = datetime.now(tz)
            for day_delta in range(1, DAYS + 1):
                date = now - timedelta(days=day_delta)
                from_dt = tz.localize(datetime(date.year, date.month, date.day, 10, 0))
                to_dt = tz.localize(datetime(date.year, date.month, date.day, 18, 45))

                try:
                    candles = client_invest.market_data.get_candles(
                        figi=figi,
                        from_=from_dt,
                        to=to_dt,
                        interval=CandleInterval.CANDLE_INTERVAL_5_MIN
                    ).candles

                    for candle in candles:
                        candle_time = candle.time.astimezone(tz)
                        close_price = float(quotation_to_decimal(candle.close))
                        candles_data.append([candle_time.strftime('%Y-%m-%d %H:%M:%S'), close_price])

                    time.sleep(0.15)
                except Exception as e:
                    print(f"❌ Ошибка загрузки {ticker} {date.date()}: {e}")

            candles_data.sort(key=lambda x: x[0])

            # Проверка на дубликаты по последним 2000 строкам
            all_dates = sheet.col_values(date_col)[7:]
            last_dates = all_dates[-MAX_EXISTING_ROWS_TO_COMPARE:]
            existing_dates = set(last_dates)

            new_candles = [row for row in candles_data if row[0] not in existing_dates]
            if not new_candles:
                print(f"✅ Нет новых данных для {ticker}")
                continue

            first_free_row = len(all_dates) + 8
            dates = [[row[0]] for row in new_candles]
            prices = [[row[1]] for row in new_candles]

            date_range = f"{rowcol_to_a1(first_free_row, date_col)}:{rowcol_to_a1(first_free_row + len(dates) - 1, date_col)}"
            price_range = f"{rowcol_to_a1(first_free_row, price_col)}:{rowcol_to_a1(first_free_row + len(prices) - 1, price_col)}"

            _, date_end_row = parse_range_bounds(date_range)
            if date_end_row:
                ensure_enough_rows(sheet, date_end_row)

            for attempt in range(MAX_RETRIES):
                try:
                    sheet.update(range_name=date_range, values=dates)
                    sheet.update(range_name=price_range, values=prices)
                    print(f"✅ Добавлено {len(new_candles)} свечей для {ticker} с {first_free_row} строки")
                    break
                except Exception as e:
                    print(f"❌ Ошибка записи {ticker}: {e}. Повтор через {DELAY} секунд...")
                    time.sleep(DELAY)
            else:
                print(f"🚫 Не удалось записать данные для {ticker} после {MAX_RETRIES} попыток")

if __name__ == "__main__":
    main()

# import gspread
# from oauth2client.service_account import ServiceAccountCredentials
# from tinkoff.invest import Client, CandleInterval
# from tinkoff.invest.utils import quotation_to_decimal
# from datetime import datetime, timedelta
# import pytz
# import time
# from google.api_core.exceptions import TooManyRequests

# GOOGLE_CREDENTIALS_FILE = "google_sheets_api.json"
# SPREADSHEET_NAME = "Спредовая торговля"
# TOKEN = "t.DdSB2WN4petW-XNHWszOo0MoW41C-Eo6v-z9T6Mx5vptze2B7NbtFQkW6Yr9iPOYs5KasDaoQAYOFdAdi67jEw"
# DAYS = 20
# tz = pytz.timezone("Europe/Moscow")

# def safe_get_cell(sheet, row, col, retries=5, delay=5):
#     for attempt in range(retries):
#         try:
#             return sheet.cell(row, col).value
#         except Exception as e:
#             print(f"Ошибка при чтении ячейки ({row}, {col}): {e}")
#             print(f"Повтор через {delay} секунд...")
#             time.sleep(delay)
#     print(f"Не удалось прочитать ячейку ({row}, {col}) после {retries} попыток")
#     return None

# def main():
#     scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
#     creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
#     client_gs = gspread.authorize(creds)
#     sheet = client_gs.open(SPREADSHEET_NAME).sheet1

#     tickers_row = 4
#     figi_row = 5
#     start_col = 3
#     num_tickers = 41

#     tickers = []
#     figis = []
#     cols_for_data = []

#     # Читаем тикеры и FIGI с задержкой, чтобы не превысить лимиты
#     for i in range(num_tickers):
#         col = start_col + i * 2
#         ticker = safe_get_cell(sheet, tickers_row, col)
#         time.sleep(0.6)
#         figi = safe_get_cell(sheet, figi_row, col)
#         time.sleep(0.6)
#         if ticker and ticker.strip() and figi and figi.strip():
#             tickers.append(ticker.strip())
#             figis.append(figi.strip())
#             cols_for_data.append((col, col + 1))

#     print(f"Найдено тикеров: {len(tickers)}")

#     with Client(TOKEN) as client_invest:
#         for ticker, figi, (date_col, price_col) in zip(tickers, figis, cols_for_data):
#             print(f"Загружаем данные для {ticker} (FIGI: {figi})")

#             candles_data = []

#             now = datetime.now(tz)
#             for day_delta in range(1, DAYS + 1):
#                 date = now - timedelta(days=day_delta)
#                 from_dt = tz.localize(datetime(date.year, date.month, date.day, 10, 0))
#                 to_dt = tz.localize(datetime(date.year, date.month, date.day, 18, 45))

#                 try:
#                     candles = client_invest.market_data.get_candles(
#                         figi=figi,
#                         from_=from_dt,
#                         to=to_dt,
#                         interval=CandleInterval.CANDLE_INTERVAL_5_MIN
#                     ).candles

#                     for candle in candles:
#                         candle_time = candle.time.astimezone(tz)
#                         close_price = float(quotation_to_decimal(candle.close))
#                         candles_data.append([candle_time.strftime('%Y-%m-%d %H:%M:%S'), close_price])

#                     time.sleep(0.15)

#                 except Exception as e:
#                     print(f"Ошибка при загрузке свечей {ticker} {date.date()}: {e}")

#             candles_data.sort(key=lambda x: x[0])

#             start_row = 8  # <-- сохраняем с 8-й строки

#             dates = [[row[0]] for row in candles_data]
#             prices = [[row[1]] for row in candles_data]

#             # Формируем диапазоны в формате A1 для записи
#             from gspread.utils import rowcol_to_a1

#             date_range = f"{rowcol_to_a1(start_row, date_col)}:{rowcol_to_a1(start_row + len(dates) -1, date_col)}"
#             price_range = f"{rowcol_to_a1(start_row, price_col)}:{rowcol_to_a1(start_row + len(prices) -1, price_col)}"

#             # Записываем данные с обработкой ошибок и задержкой
#             for _ in range(5):
#                 try:
#                     sheet.update(date_range, dates)
#                     sheet.update(price_range, prices)
#                     print(f"Данные записаны для {ticker} в столбцы {date_col} (дата), {price_col} (цена)")
#                     break
#                 except Exception as e:
#                     print(f"Ошибка при записи данных для {ticker}: {e}")
#                     print("Повтор через 10 секунд...")
#                     time.sleep(10)
#             else:
#                 print(f"Не удалось записать данные для {ticker} после нескольких попыток")

# if __name__ == "__main__":
#     main()
