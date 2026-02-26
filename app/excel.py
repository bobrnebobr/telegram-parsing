import openpyxl
from openpyxl.utils import get_column_letter

HEADERS = [
    "Порядковый номер ЭД в реестре",
    "Номер файла в ЭД",
    "Регистрационный номер ЭД",
    "Дата регистрации ЭД",
    "Вид ЭД",
    "Наименование (заголовок) электронного документа",
    "Наименование файла",
    "Дата и время последнего изменения файла",
    "Объем файла",
    "Формат файла",
    "Контрольная сумма файла",
    "Путь к файлу",
]


def save_excel(path, rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Реестр"

    ws.append(HEADERS)

    for row in rows:
        ws.append(row)

    # автоширина
    for i, col in enumerate(ws.columns, 1):
        max_len = max(len(str(cell.value)) if cell.value else 0 for cell in col)
        ws.column_dimensions[get_column_letter(i)].width = min(max_len + 2, 80)

    wb.save(path)