import asyncio
from .exporter import ChannelExporter


async def main():
    exporter = ChannelExporter()
    await exporter.run()


if __name__ == "__main__":
    asyncio.run(main())
