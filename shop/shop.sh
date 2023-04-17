#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

lock_file="/tmp/shop_`date '+%Y%m%d%H'`.lock"
if [ -f "${lock_file}" ];then
    exit 1
fi
touch "${lock_file}"
trap "[ -f \"${lock_file}\" ] && rm -f ${lock_file}" HUP INT QUIT TSTP EXIT

# 任务执行（每分钟启动）
pushd /app/scripts/shop
    python3 shop_traffic.py &
    python3 shop_trans.py &
    python3 shop_trend.py &
    python3 shop_flow_source.py
popd
wait

exit 0