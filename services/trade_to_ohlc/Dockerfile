from python:3.10.3-slim-buster

ENV PYTHONPATH=/app

ENV PYTHONUNBUFFERED = 1

RUN pip install poetry

WORKDIR /app

COPY poetry.lock pyproject.toml /app/

RUN poetry install

COPY . /app

CMD ["poetry", "run", "python", "src/main.py"]