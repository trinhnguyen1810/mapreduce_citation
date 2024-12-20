FROM python:3.8
WORKDIR /app
COPY . /app/
RUN pip install flask requests
ENV PORT=5001
CMD ["python", "master.py"]

