import openpyxl

def save_excel(path, rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["post_id", "date", "author", "text", "media_count", "folder"])

    for row in rows:
        ws.append(row)

    wb.save(path)
