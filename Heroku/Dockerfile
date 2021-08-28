FROM python:3
ENV PYTHONUNBUFFERED 1
RUN mkdir /app
WORKDIR /app
COPY requirements.txt /app
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r requirements.txt
COPY . /app
EXPOSE 10000
CMD [ "python", "TestApp.py" ]