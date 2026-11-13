FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY services/rsu/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && mkdir -p /data
COPY services/__init__.py services/__init__.py
COPY services/rsu/ services/rsu/
COPY schema/ schema/
EXPOSE 8090
CMD ["uvicorn", "services.rsu.app:app", "--host", "0.0.0.0", "--port", "8090"]
