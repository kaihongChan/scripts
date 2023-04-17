#!/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

# 每3时投递任务
pushd /app/scripts/shipment
    python3 shipment_job.py
popd