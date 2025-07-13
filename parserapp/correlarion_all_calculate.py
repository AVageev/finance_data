import gspread
from oauth2client.service_account import ServiceAccountCredentials
from itertools import combinations
import pandas as pd
import time
import json

GOOGLE_CREDENTIALS_FILE = "google_sheets_api.json"
SPREADSHEET_NAME = "Спредовая торговля"


def get_tickers():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
    client_gs = gspread.authorize(creds)
    sheet = client_gs.open(SPREADSHEET_NAME).sheet1

    tickers_row = 4
    start_col = 3
    num_tickers = 41

    tickers = []
    cols = []
    for i in range(num_tickers):
        col = start_col + i * 2
        ticker = sheet.cell(tickers_row, col).value
        if ticker:
            tickers.append(ticker.strip())
            cols.append(col)

    return tickers, cols, sheet


def are_tickers_too_similar(t1, t2):
    if t1 == t2:
        return True
    if t1.startswith(t2) or t2.startswith(t1):
        return True
    return False


def get_price_series(sheet, col, rows=2000):
    start_row = 8
    end_row = start_row + rows - 1

    price_col = col + 1
    cell_range = f"{gspread.utils.rowcol_to_a1(start_row, price_col)}:{gspread.utils.rowcol_to_a1(end_row, price_col)}"
    print(f"\nЗапрашиваем диапазон цен: {cell_range}")

    values = sheet.get(cell_range)
    print(f"Получено строк: {len(values)}")

    prices = []
    for row in values:
        if row and row[0]:
            val = row[0].replace(',', '.')
            try:
                prices.append(float(val))
            except Exception as e:
                print(f"Ошибка преобразования значения '{row[0]}' в float: {e}")
                prices.append(None)
        else:
            prices.append(None)

    print(f"Полный список цен ({len(prices)}): {prices}")
    return pd.Series(prices)


def calculate_daily_growth(price_series, periods=104):
    """
    Рассчитать рост за последние `periods` значений.
    Формула: (последняя цена - цена periods назад) / цена periods назад * 100%
    Возвращает None если данных недостаточно или есть пропуски.
    """
    if len(price_series) < periods + 1:
        return None

    recent = price_series.dropna().tail(periods + 1)
    if len(recent) < periods + 1:
        return None

    start_price = recent.iloc[0]
    end_price = recent.iloc[-1]

    if start_price == 0 or start_price is None or end_price is None:
        return None

    growth = (end_price - start_price) / start_price * 100
    return growth


def main():
    tickers, cols, sheet = get_tickers()
    print(f"Всего тикеров: {len(tickers)}")

    # Фильтрация пар тикеров для корреляции
    pairs = []
    for t1, t2 in combinations(zip(tickers, cols), 2):
        if are_tickers_too_similar(t1[0], t2[0]):
            continue
        pairs.append((t1, t2))

    print(f"Всего пар после фильтра: {len(pairs)}")

    data_cache = {}
    growth_cache = {}

    # Загрузка цен и подсчёт роста для каждого тикера
    for ticker, col in zip(tickers, cols):
        print(f"\nЗагружаем данные для {ticker} (столбец {col})")
        series = get_price_series(sheet, col, 2000)
        data_cache[ticker] = series

        growth = calculate_daily_growth(series, periods=104)
        growth_cache[ticker] = growth
        print(f"Рост за последние сутки для {ticker}: {growth if growth is not None else 'нет данных'}%")

        time.sleep(5)

    print("\nДанные по тикерам загружены, считаем корреляции...\n")

    result_json = []
    for (t1, c1), (t2, c2) in pairs:
        s1 = data_cache[t1]
        s2 = data_cache[t2]
        df = pd.concat([s1, s2], axis=1).dropna()
        if len(df) == 0:
            corr = None
        else:
            corr = df.corr().iloc[0, 1]

        result = {
            "ticker1": t1,
            "ticker2": t2,
            "correlation": corr
        }
        result_json.append(result)
        print(f"Корреляция {t1} и {t2}: {corr if corr is not None else 'нет данных'}")

    # Сохраняем корреляции в файл
    with open("correlations.json", "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=2)
    print("\nРезультаты корреляций сохранены в correlations.json")

    # Сохраняем рост в отдельный JSON для лидеров роста/падения
    growth_list = [{"ticker": t, "growth_percent": g} for t, g in growth_cache.items()]
    with open("growth.json", "w", encoding="utf-8") as f:
        json.dump(growth_list, f, ensure_ascii=False, indent=2)
    print("Результаты роста сохранены в growth.json")


if __name__ == "__main__":
    main()






# import gspread
# from oauth2client.service_account import ServiceAccountCredentials
# from itertools import combinations
# import pandas as pd
# import time
# import json

# GOOGLE_CREDENTIALS_FILE = "google_sheets_api.json"
# SPREADSHEET_NAME = "Спредовая торговля"


# def get_tickers():
#     scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
#     creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
#     client_gs = gspread.authorize(creds)
#     sheet = client_gs.open(SPREADSHEET_NAME).sheet1

#     tickers_row = 4
#     start_col = 3
#     num_tickers = 41

#     tickers = []
#     cols = []
#     for i in range(num_tickers):
#         col = start_col + i * 2
#         ticker = sheet.cell(tickers_row, col).value
#         if ticker:
#             tickers.append(ticker.strip())
#             cols.append(col)

#     return tickers, cols, sheet


# def are_tickers_too_similar(t1, t2):
#     if t1 == t2:
#         return True
#     if t1.startswith(t2) or t2.startswith(t1):
#         return True
#     return False


# def get_price_series(sheet, col, rows=2000):
#     start_row = 8
#     end_row = start_row + rows - 1

#     price_col = col + 1
#     cell_range = f"{gspread.utils.rowcol_to_a1(start_row, price_col)}:{gspread.utils.rowcol_to_a1(end_row, price_col)}"
#     print(f"\nЗапрашиваем диапазон цен: {cell_range}")

#     values = sheet.get(cell_range)
#     print(f"Получено строк: {len(values)}")

#     prices = []
#     for row in values:
#         if row and row[0]:
#             val = row[0].replace(',', '.')
#             try:
#                 prices.append(float(val))
#             except Exception as e:
#                 print(f"Ошибка преобразования значения '{row[0]}' в float: {e}")
#                 prices.append(None)
#         else:
#             prices.append(None)

#     print(f"Полный список цен ({len(prices)}): {prices}")
#     return pd.Series(prices)


# def main():
#     tickers, cols, sheet = get_tickers()
#     print(f"Всего тикеров: {len(tickers)}")

#     pairs = []
#     for t1, t2 in combinations(zip(tickers, cols), 2):
#         if are_tickers_too_similar(t1[0], t2[0]):
#             continue
#         pairs.append((t1, t2))

#     print(f"Всего пар после фильтра: {len(pairs)}")

#     data_cache = {}
#     for ticker, col in zip(tickers, cols):
#         print(f"\nЗагружаем данные для {ticker} (столбец {col})")
#         series = get_price_series(sheet, col, 2000)
#         data_cache[ticker] = series
#         time.sleep(5)

#     print("\nДанные по тикерам загружены, считаем корреляции...\n")

#     result_json = []

#     for (t1, c1), (t2, c2) in pairs:
#         s1 = data_cache[t1]
#         s2 = data_cache[t2]
#         df = pd.concat([s1, s2], axis=1).dropna()
#         if len(df) == 0:
#             corr = None
#         else:
#             corr = df.corr().iloc[0, 1]

#         result = {
#             "ticker1": t1,
#             "ticker2": t2,
#             "correlation": corr
#         }
#         result_json.append(result)
#         print(f"Корреляция {t1} и {t2}: {corr if corr is not None else 'нет данных'}")

#     with open("correlations.json", "w", encoding="utf-8") as f:
#         json.dump(result_json, f, ensure_ascii=False, indent=2)
#     print("\nРезультаты сохранены в correlations.json")


# if __name__ == "__main__":
#     main()

