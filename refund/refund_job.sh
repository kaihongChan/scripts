#!/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

# 每3小时投递任务
pushd /app/scripts/refund
    python3 refund_job.py
popd