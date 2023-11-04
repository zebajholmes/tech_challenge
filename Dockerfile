FROM python:3.7

RUN pip install --upgrade pip
RUN pip install psycopg2-binary && pip install shapely

RUN mkdir /src

COPY src src

CMD python3 /src/main.py