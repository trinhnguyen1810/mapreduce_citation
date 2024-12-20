FROM python:3.8
WORKDIR /app
COPY . /app/
RUN pip install flask requests
ENV WORKER_PORT=5002
ENV MASTER_HOST=master
ENV MASTER_PORT=5001
CMD ["python", "mapper.py"]
