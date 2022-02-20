FROM python:3.8

COPY requirements.txt .
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

COPY ./solution/script ./solution/script/
COPY ./data/ ./data/

WORKDIR /solution/script/
CMD ["python", "main.py"]