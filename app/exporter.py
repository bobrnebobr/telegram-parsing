import os
import hashlib
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient
from openpyxl import Workbook
import magic

from .config import API_ID, API_HASH, CHANNEL_NAME, OUTPUT_DIR, SESSION_NAME


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


def make_dir(path):
    os.makedirs(path, exist_ok=True)


def detect_file_type(file_path: Path):
    ext = file_path.suffix.lower()

    try:
        mime = magic.from_file(str(file_path), mime=True)
    except Exception:
        mime = None

    # Фото
    if mime and mime.startswith("image/"):
        return "Фотодокумент", mime

    # Видео
    elif mime and mime.startswith("video/"):
        return "Видеодокумент", mime

    # Аудио
    elif mime and mime.startswith("audio/"):
        return "Аудиодокумент", mime

    # Текстовые файлы
    elif ext == ".txt":
        return "", "text/plain"

    # Всё остальное
    else:
        return "Иное", mime if mime else "application/octet-stream"


class ChannelExporter:
    def __init__(self):
        self.client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        self.global_index = 0
        self.year_books = {}
        self.channel_root = None
        self.channel_name = None

    async def run(self):
        async with self.client:
            entity = await self.client.get_entity(CHANNEL_NAME)
            self.channel_name = entity.title.replace("/", "_")

            self.channel_root = os.path.join(OUTPUT_DIR, self.channel_name)
            make_dir(self.channel_root)

            current_group = None
            buffer = []

            async for msg in self.client.iter_messages(entity, reverse=True):
                if not msg.date:
                    continue

                if msg.grouped_id:
                    if current_group is None:
                        current_group = msg.grouped_id

                    if msg.grouped_id == current_group:
                        buffer.append(msg)
                        continue
                    else:
                        await self.process_post(buffer)
                        buffer = [msg]
                        current_group = msg.grouped_id
                        continue

                if buffer:
                    await self.process_post(buffer)
                    buffer = []
                    current_group = None

                await self.process_post([msg])
                print(f"Пост {msg.id} сохранен")

            if buffer:
                await self.process_post(buffer)

        self.save_all()

    async def process_post(self, messages):
        media_messages = [m for m in messages if m.media]
        if not media_messages:
            return  # берём только посты с медиа

        main_msg = messages[0]
        post_id = main_msg.id
        post_date = main_msg.date

        year = post_date.strftime("%Y")
        month = post_date.strftime("%m")

        post_path = os.path.join(self.channel_root, month, str(post_id))
        make_dir(post_path)

        # Текст поста
        full_text = ""
        for m in messages:
            if m.text:
                full_text += m.text + "\n\n"
        full_text = full_text.strip()

        # Скачиваем медиа
        saved_files = []
        for m in media_messages:
            saved = await m.download_media(file=post_path)
            if isinstance(saved, list):
                saved_files.extend(saved)
            else:
                saved_files.append(saved)

        if not saved_files:
            return

        self.global_index += 1
        ws = self.get_year_sheet(year)
        file_number = 0

        # 1️⃣ Медиа-файлы
        for file_path in saved_files:
            file_number += 1
            ws.append(self.build_row(
                ed_number=self.global_index if file_number == 1 else "",
                file_number=file_number,
                post_id=post_id,
                post_date=post_date,
                title=full_text,
                file_path=file_path
            ))

        # 2️⃣ Текстовый файл
        text_filename = f"{post_id}.txt"
        text_path = os.path.join(post_path, text_filename)

        with open(text_path, "w", encoding="utf-8") as f:
            f.write(full_text)

        file_number += 1
        ws.append(self.build_row(
            ed_number="",
            file_number=file_number,
            post_id=post_id,
            post_date=post_date,
            title=full_text,
            file_path=text_path
        ))

    def build_row(self, ed_number, file_number, post_id, post_date,
                  title, file_path):

        stat = os.stat(file_path)
        file_modified = datetime.fromtimestamp(stat.st_mtime)
        doc_type, file_format = detect_file_type(Path(file_path))

        # Относительный путь от папки канала
        relative_path = "./" + str(os.path.relpath(file_path, self.channel_root))

        return [
            ed_number,
            file_number,
            post_id,  # регистрационный номер = id Telegram
            post_date.strftime("%Y-%m-%d %H:%M:%S"),
            doc_type,
            title,
            os.path.basename(file_path),
            file_modified.strftime("%Y-%m-%d %H:%M:%S"),
            stat.st_size,
            file_format,
            self.sha256(file_path),
            relative_path
        ]

    def get_year_sheet(self, year):
        if year not in self.year_books:
            year_path = os.path.join(self.channel_root, year)
            make_dir(year_path)

            wb = Workbook(write_only=True)
            ws = wb.create_sheet("Реестр")
            ws.append(HEADERS)

            self.year_books[year] = (wb, ws)

        return self.year_books[year][1]

    def save_all(self):
        for year, (wb, _) in self.year_books.items():
            save_path = os.path.join(
                self.channel_root,
                year,
                f"index_{year}.xlsx"
            )
            wb.save(save_path)

    @staticmethod
    def sha256(path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()