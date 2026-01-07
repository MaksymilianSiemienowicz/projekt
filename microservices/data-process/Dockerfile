FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY main.py .
ENV FLOW_ID=65f1bbb00000000000000002
CMD ["python", "main.py"]
