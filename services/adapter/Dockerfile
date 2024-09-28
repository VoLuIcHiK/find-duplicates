FROM python:3.11

RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y

WORKDIR /app

COPY ./services/adapter/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY ./services/adapter adapter/
COPY ./configs configs/

ENV LOGURU_COLORIZE "true"

CMD ["python", "-m", "adapter"]