FROM python:3.8-slim as base
RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt
COPY src/ .
CMD ["python",  "-m",  "prozorro_crawler.main"]
