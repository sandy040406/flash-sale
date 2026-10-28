FROM python:3.12-alpine
RUN apk add --no-cache docker-cli docker-cli-compose
COPY autoscaler.py /autoscaler.py
CMD ["python", "-u", "/autoscaler.py"]
