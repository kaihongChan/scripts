#!/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH
# 每月更新一次（店铺信息）
pushd /app/scripts/shop
    python3 shop_info_job.py
popd