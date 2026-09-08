FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLBACKEND=Agg \
    MPLCONFIGDIR=/tmp/matplotlib \
    RECOMMEND_DB=/data/recommendations.sqlite3

WORKDIR /code
COPY requirements.txt requirements.lock ./
RUN python -m pip install --no-cache-dir --only-binary=:all: -r requirements.lock \
    && python -m pip check

RUN useradd --create-home --uid 10001 bot
COPY app.py recommendation_service.py signal_runner.py ./
COPY api_data_service/ ./api_data_service/
COPY analytics/ ./analytics/
COPY config/recommendations.json ./config/recommendations.json
RUN mkdir -p /data && chown -R bot:bot /data /code/config
USER bot
CMD ["python", "app.py"]
