FROM python:3.12
LABEL authors="Sergey"

WORKDIR /app

COPY ./requirements.txt ./

RUN pip3 install -r requirements.txt

COPY ./ ./ultra_backend

ENV LOGURU_COLORIZE="true"

ENTRYPOINT ["python", "-m", "ultra_backend.fast_answer"]