from python:3.10.3-slim-buster

ENV PYTHONUNBUFFERED = 1

RUN pip install poetry

WORKDIR /app

COPY . /app

RUN poetry install

CMD ["poetry", "run", "python", "src/main.py"]