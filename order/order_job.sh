#!/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

# 每3小时投递（订单采集）任务
pushd /app/scripts/order
    python3 order_job.py
popd