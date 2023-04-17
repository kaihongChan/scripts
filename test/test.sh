#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

# 测试定时任务
pushd /app/scripts/test
    python3 test.py
popd
