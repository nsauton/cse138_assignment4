FROM python:3.12.2-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8081
CMD ["uvicorn", "server:server", "--host", "0.0.0.0", "--port", "8081"]