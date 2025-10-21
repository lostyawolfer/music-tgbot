import asyncio
from db.db import Music
from handlers import user_menu
from data.loader import *

async def main():
    dp.include_router(user_menu.router)
    await dp.start_polling(bot)

if __name__ == '__main__':
    db = Music()
    db.createdb()
    asyncio.run(main())