FROM python:3.7-slim
RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt
COPY ./src /app
CMD ["python", "/app/main.py"]