FROM python:3.9

WORKDIR /app

# As the context is the root, we copy from nar-nare/
COPY requirements.txt .
COPY telemedida_service.py .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

EXPOSE 8020

CMD ["python", "telemedida_service.py"]