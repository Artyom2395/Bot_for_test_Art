from datetime import timedelta
from dateutil import parser
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient


# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client['sampledb']
collection = db['collection_1']

async def aggregate_salary_data(dt_from, dt_upto, group_type):
    # Преобразование входных дат в объекты даты и времени
    dt_from = parser.isoparse(dt_from)
    dt_upto = parser.isoparse(dt_upto)

    # Определение временного диапазона для агрегации
    time_range = []
    current_time = dt_from

    if group_type == "hour":
        delta = timedelta(hours=1)
    elif group_type == "day":
        delta = timedelta(days=1)
    elif group_type == "month":
        delta = relativedelta(months=1)
    else:
        raise ValueError("Invalid group_type")

    while current_time <= dt_upto:
        time_range.append(current_time)
        current_time += delta

    # Выполнение агрегации
    dataset = []
    labels = []

    
    for time in time_range:
        start_time = time
        if start_time == time_range[-1]:
            end_time = dt_upto
            if group_type == "month":
                end_time = time + delta
        else:
            end_time = time + delta

        aggregation_pipeline = [
            {
                "$match": {
                    "dt": {"$gte": start_time, "$lt": end_time}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_salary": {"$sum": "$value"}
                }
            }
        ]

        result = collection.aggregate(aggregation_pipeline)
        total_salary = next(result, {"total_salary": 0})["total_salary"]

        dataset.append(total_salary)
        labels.append(time.isoformat())
    
    return {"dataset": dataset, "labels": labels}