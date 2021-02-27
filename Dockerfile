FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src .
ENV PYTHONPATH=/app
EXPOSE 8888

CMD uvicorn --port 8888 --host 0.0.0.0 main:app
