import os
from telethon import TelegramClient
from .utils import make_dir
from .excel import save_excel
from .config import (
    API_ID,
    API_HASH,
    CHANNEL_NAME,
    OUTPUT_DIR,
    SESSION_NAME,
)


class ChannelExporter:
    def __init__(self):
        self.client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        self.year_data = {}
        self.month_data = {}

    async def run(self):
        async with self.client:
            entity = await self.client.get_entity(CHANNEL_NAME)
            channel_name = entity.title.replace("/", "_")

            root = os.path.join(OUTPUT_DIR, channel_name)
            make_dir(root)

            async for msg in self.client.iter_messages(entity):
                if not msg.date:
                    continue

                await self.process_message(msg, root)

            self.save_indexes(root)

    async def process_message(self, msg, root):
        year = msg.date.strftime("%Y")
        month = msg.date.strftime("%m")

        year_path = os.path.join(root, year)
        month_path = os.path.join(year_path, month)
        post_path = os.path.join(month_path, str(msg.id))

        make_dir(post_path)

        # автор
        author = str(msg.sender_id) if msg.sender_id else ""

        # текст
        text_file = os.path.join(post_path, "post.txt")
        with open(text_file, "w", encoding="utf-8") as f:
            f.write(f"Date: {msg.date}\n")
            f.write(f"Author: {author}\n\n")
            f.write(msg.text or "")

        # медиа
        media_count = 0
        if msg.media:
            await msg.download_media(file=post_path)
            media_count = 1

        row = [
            msg.id,
            msg.date.strftime("%Y-%m-%d %H:%M:%S"),
            author,
            (msg.text or "")[:2000],
            media_count,
            post_path,
        ]

        self.year_data.setdefault(year, []).append(row)
        self.month_data.setdefault((year, month), []).append(row)

        print(f"Saved post {msg.id}")

    def save_indexes(self, root):
        for year, rows in self.year_data.items():
            year_path = os.path.join(root, year)
            save_excel(os.path.join(year_path, f"index_{year}.xlsx"), rows)

        for (year, month), rows in self.month_data.items():
            month_path = os.path.join(root, year, month)
            save_excel(
                os.path.join(month_path, f"index_{year}_{month}.xlsx"),
                rows,
            )
