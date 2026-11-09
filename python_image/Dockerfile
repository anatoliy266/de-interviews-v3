FROM python:3.14.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt-get/lists/*

COPY python_image/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

CMD ["tail", "-f", "/dev/null"]