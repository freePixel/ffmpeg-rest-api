FROM python:3.12

WORKDIR /app

RUN apt-get update
RUN apt-get install -y ffmpeg

RUN mkdir -p /api_data/files

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

RUN python3 -m pytest

CMD ["python3", "src/app.py"]
