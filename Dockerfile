FROM python:3.7-slim as base
RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt
COPY src/ .
CMD ["python",  "-m",  "prozorro_crawler.main"]