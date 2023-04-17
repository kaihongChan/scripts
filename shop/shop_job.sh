#!/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

# 每天05:00投递任务（采集店铺统计数据）
pushd /app/scripts/shop
    python3 shop_job.py
popd