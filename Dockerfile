FROM python:3.8-slim

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY sources.list /etc/apt/sources.list
RUN  apt-get update \
    && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /app/scripts
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt \
    --no-cache-dir \
    --trusted-host mirrors.aliyun.com \
    --index-url "https://mirrors.aliyun.com/pypi/simple/"

COPY . .
RUN crontab cronfile
RUN chmod +x entrypoint.sh
ENV LC_ALL C.UTF-8
ENTRYPOINT ["./entrypoint.sh"]
