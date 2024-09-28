FROM python:3.12

WORKDIR /app

COPY ./services/new_backend/requirements.txt  ./

RUN pip install --no-cache-dir -r requirements.txt

COPY ./services/new_backend ./
COPY ./configs ./configs

ENV LOGURU_COLORIZE="true"

CMD ["python", "main.py"]