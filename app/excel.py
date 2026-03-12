from openpyxl import Workbook
import os

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

def export_excel(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Реестр")
    ws.append(HEADERS)
    for r in rows:
        ws.append(r)
    wb.save(path)