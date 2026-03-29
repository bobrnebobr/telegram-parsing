import os
import asyncio
import hashlib
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import FileReferenceExpiredError, FloodWaitError, RPCError
from telethon.network.connection.tcpmtproxy import ConnectionTcpMTProxyRandomizedIntermediate
import magic
import socks
import binascii

from .config import API_ID, API_HASH, CHANNEL_NAME, OUTPUT_DIR, SESSION_NAME, DB_PATH
from .db import RegistryDB
from .excel import export_excel

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
    if mime and mime.startswith("image/"):
        return "Фотодокумент", mime
    elif mime and mime.startswith("video/"):
        return "Видеодокумент", mime
    elif mime and mime.startswith("audio/"):
        return "Аудиодокумент", mime
    elif ext == ".txt":
        return "", "text/plain"
    else:
        return "Иное", mime if mime else "application/octet-stream"

class ChannelExporter:

    def __init__(self, max_parallel=4):
        self.proxy = (
            os.getenv("PROXY_HOST"),
            int(os.getenv("PROXY_PORT")),
            binascii.unhexlify(os.getenv("PROXY_SECRET"))
        )
        self.client = TelegramClient(
            SESSION_NAME,
            API_ID,
            API_HASH,
            connection=ConnectionTcpMTProxyRandomizedIntermediate,
            proxy=self.proxy)
        self.db = RegistryDB(DB_PATH)
        self.semaphore = asyncio.Semaphore(max_parallel)
        self.global_index = 0
        self.channel_root = None
        self.channel_name = None
        self.current_year = None
        self.current_month = None

    def get_last_post_id_from_files(self):
        max_id = 0
        if not os.path.exists(self.channel_root):
            return None
        for year in os.listdir(self.channel_root):
            year_path = os.path.join(self.channel_root, year)
            if not os.path.isdir(year_path):
                continue
            for month in os.listdir(year_path):
                month_path = os.path.join(year_path, month)
                if not os.path.isdir(month_path):
                    continue
                for post_id in os.listdir(month_path):
                    if post_id.isdigit():
                        max_id = max(max_id, int(post_id))
        return max_id if max_id > 0 else None

    async def safe_download(self, message, path):
        async with self.semaphore:
            for attempt in range(3):
                try:
                    return await message.download_media(file=path)
                except FileReferenceExpiredError:
                    message = await self.client.get_messages(message.chat_id, ids=message.id)
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                except RPCError:
                    await asyncio.sleep(2)
            return None

    async def run(self):
        async with self.client:
            entity = await self.client.get_entity(CHANNEL_NAME)
            self.channel_name = entity.title.replace("/", "_")
            self.channel_root = os.path.join(OUTPUT_DIR, self.channel_name)
            make_dir(self.channel_root)
            current_group = None
            buffer = []

            last_post_id = self.get_last_post_id_from_files()
            min_id = (last_post_id - 10) if last_post_id else 0
            print(f"Resuming from post_id > {min_id}")

            async for msg in self.client.iter_messages(entity, reverse=True, min_id=min_id):
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
            if buffer:
                await self.process_post(buffer)
        self.finalize_exports()

    async def process_post(self, messages):
        media_messages = [m for m in messages if m.media]
        if not media_messages:
            return
        main_msg = messages[0]
        post_id = main_msg.id
        post_date = main_msg.date
        year = post_date.strftime("%Y")
        month = post_date.strftime("%m")

        post_path = os.path.join(
            self.channel_root,
            post_date.strftime("%Y"),
            post_date.strftime("%m"),
            str(post_id)
        )

        if os.path.exists(post_path) and os.listdir(post_path):
            print(f"[SKIP] Post {post_id} already exists")
            return

        if self.current_year is None:
            self.current_year = year
            self.current_month = month
        await self.check_rotation(year, month)
        post_path = os.path.join(self.channel_root, year, month, str(post_id))
        make_dir(post_path)
        full_text = "\n\n".join(m.text for m in messages if m.text).strip()
        tasks = [self.safe_download(m, post_path) for m in media_messages]
        results = await asyncio.gather(*tasks)
        saved_files = []
        for r in results:
            if isinstance(r, list):
                saved_files.extend(r)
            elif r:
                saved_files.append(r)
        if not saved_files:
            return
        self.global_index += 1
        file_number = 0
        for file_path in saved_files:
            file_number += 1
            row = self.build_row(
                self.global_index if file_number == 1 else None,
                file_number,
                post_id,
                post_date,
                full_text,
                file_path
            )
            self.db.insert((row[0], row[1], row[2], row[3],
                            year, month,
                            row[4], row[5], row[6], row[7],
                            row[8], row[9], row[10], row[11]))
        text_path = os.path.join(post_path, f"{post_id}.txt")
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(full_text)
        print(f"Post {post_id} succesfully saved")

    async def check_rotation(self, year, month):
        if month != self.current_month:
            rows = self.db.fetch_month(self.current_year, self.current_month)
            if rows:
                path = os.path.join(self.channel_root, self.current_year, self.current_month, f"index_{self.current_year}_{self.current_month}.xlsx")
                export_excel(rows, path)
                self.db.delete_month(self.current_year, self.current_month)
            self.current_month = month
        if year != self.current_year:
            rows = self.db.fetch_year(self.current_year)
            if rows:
                path = os.path.join(self.channel_root, self.current_year, f"index_{self.current_year}.xlsx")
                export_excel(rows, path)
            self.current_year = year

    def finalize_exports(self):
        if not self.current_year or not self.current_month:
            return
        rows = self.db.fetch_month(self.current_year, self.current_month)
        if rows:
            path = os.path.join(self.channel_root, self.current_year, self.current_month, f"index_{self.current_year}_{self.current_month}.xlsx")
            export_excel(rows, path)
        rows = self.db.fetch_year(self.current_year)
        if rows:
            path = os.path.join(self.channel_root, self.current_year, f"index_{self.current_year}.xlsx")
            export_excel(rows, path)

    def build_row(self, ed_number, file_number, post_id, post_date, title, file_path):
        stat = os.stat(file_path)
        file_modified = datetime.fromtimestamp(stat.st_mtime)
        doc_type, file_format = detect_file_type(Path(file_path))
        relative_path = "./" + str(os.path.relpath(file_path, self.channel_root))
        return [
            ed_number,
            file_number,
            post_id,
            post_date.strftime("%Y-%m-%d %H:%M:%S"),
            doc_type,
            title if title else None,
            os.path.basename(file_path),
            file_modified.strftime("%Y-%m-%d %H:%M:%S"),
            stat.st_size,
            file_format,
            self.sha256(file_path),
            relative_path
        ]

    @staticmethod
    def sha256(path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
