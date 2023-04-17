#!/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

# 每天投递任务
pushd /app/scripts/ware
    python3 item_job.py
popd
