FROM python:3.6-slim
RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt
COPY src/ .
CMD ["python",  "-m",  "prozorro_crawler.main"]