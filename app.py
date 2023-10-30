
import logging
import json
from environs import Env
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from aggregate import aggregate_salary_data

env = Env() 
env.read_env() 

# Инициализация бота
bot = Bot(token=env("TOKEN"))
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)


@dp.message(Command(commands=['start']))
async def process_start_command(message: Message):
    await message.answer('Привет!\nВводи данные')
    
@dp.message()
async def on_text(message: Message):
    try:
        # Пытаемся распарсить входные данные как JSON
        data = json.loads(message.text)
        
        dt_from = data.get("dt_from")
        dt_upto = data.get("dt_upto")
        group_type = data.get("group_type")

        if not dt_from or not dt_upto or not group_type:
            await message.answer("Пожалуйста, укажите корректные входные данные в формате JSON.")
            return

        result = await aggregate_salary_data(dt_from, dt_upto, group_type)

        response = json.dumps(result, ensure_ascii=False)

        await message.answer(response)
    except Exception as e:
        await message.answer(f"Произошла ошибка: {str(e)}")

if __name__ == '__main__':
    dp.run_polling(bot)
