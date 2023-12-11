import asyncio
from contextlib import asynccontextmanager
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload
import uvicorn
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import List
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
import logging

import settings


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("app.log")
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)


logger.addHandler(file_handler)


# Создание приложения FastAPI
def create_app():
    app = FastAPI(docs_url='/')

    # Модель данных для хранения результатов
    Base = declarative_base()

    class Result(Base):
        __tablename__ = 'results'
        id = Column(Integer, primary_key=True)
        datetime = Column(String)
        title = Column(String)
        x_avg_count_in_line = Column(Float)
        text = Column(String)

        def __init__(self, datetime, title, x_avg_count_in_line, text):
            self.datetime = datetime
            self.title = title
            self.x_avg_count_in_line = x_avg_count_in_line
            self.text = text

    # Подключение к БД

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        ml_models = {}

        # Загрузка модели машинного обучения
        def fake_answer_to_everything_ml_model(x: float):
            return x * 42

        ml_models["answer_to_everything"] = fake_answer_to_everything_ml_model
        yield
        # Очистка моделей машинного обучения и освобождение ресурсов
        ml_models.clear()

    def calculate_x_avg_count(result):
        lines = result["text"].split('\n')
        x_count = 0
        total_lines = 0
        for line in lines:
            if 'X' in line:
                x_count += 1
            total_lines += 1
        if total_lines > 0:
            x_avg_count_in_line = x_count / total_lines
        else:
            x_avg_count_in_line = 0
        print("Calculated x_avg_count_in_line:", x_avg_count_in_line)
        return x_avg_count_in_line

    async def process_data(result, session):
        x_avg_count_in_line = calculate_x_avg_count(result)
        result[
            "x_avg_count_in_line"] = x_avg_count_in_line
        logger.info("Updated Result object: %s", result)
        result_obj = Result(**result)
        session.add(result_obj)
        await session.commit()
        logger.info("Saving Result object: %s", result)  # Add this line for debugging

    @app.on_event("startup")
    async def startup_event():


        # Создание асинхронного движка
        engine = create_async_engine(settings.db_url, future=True)


        # Создание таблицы в базе данных
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)


        # Асинхронная функция для отправки данных в брокер сообщений
        async def send_data(engine):
            async with engine.begin() as connection:
                session = AsyncSession(bind=connection)

                while True:
                    try:
                        async with session.begin():
                            await session.execute(text("SELECT 1"))
                        logger.info("Successful connection to the database")
                    except Exception as e:
                        logger.error("Database connection error: %s", e)

                    data = {
                        "datetime": datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f"),
                        "title": "Very fun book",
                        "text": "...Rofl...lol...\n..ololo..X.."
                    }
                    result = {
                        "datetime": data["datetime"],
                        "title": data["title"],
                        "text": data["text"]
                    }
                    logger.info("Sending data: %s", result)

                    async with session.begin():
                        print("Data being saved:", result)
                        await process_data(result, session)

                    await asyncio.sleep(3)

        asyncio.create_task(send_data(engine))

    # Определение функции get_results
    @app.get("/results")
    async def get_results():
        engine = create_async_engine(settings.db_url, future=True)
        async with engine.begin() as connection:
            async with AsyncSession(bind=connection) as session:
                results = await session.execute(select(Result))
                results = results.scalars().all()
                if results:
                    output = []
                    for result in results:
                        output.append({
                            "datetime": result.datetime,
                            "title": result.title,
                            "x_avg_count_in_line": result.x_avg_count_in_line,
                            "text": result.text
                        })
                    return {"message": "Данные найдены в базе данных", "results": output}
                else:
                    return {"message": "В базе данных нет данных"}
    return app


app = create_app()


def run_app():
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)


if __name__ == '__main__':
    run_app()