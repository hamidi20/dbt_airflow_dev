FROM python:3.11-slim

WORKDIR /usr/app

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

RUN pip install --no-cache-dir \
    dbt-core==1.8.2 \
    dbt-postgres==1.8.2 \
    pytz

CMD ["tail","-f","/dev/null"]