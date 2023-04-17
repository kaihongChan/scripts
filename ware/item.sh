#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

lock_file="/tmp/item_`date '+%Y%m%d%H'`.lock"
if [ -f "${lock_file}" ];then
    exit 1
fi
touch "${lock_file}"
trap "[ -f \"${lock_file}\" ] && rm -f ${lock_file}" HUP INT QUIT TSTP EXIT

# 定时每分钟执行
pushd /app/scripts/ware
    python3 item_flow_rank.py &
    python3 items_indicate.py &
    python3 item_overview.py &
    python3 item_traffic.py &
    python3 item_trend.py
popd
wait

exit 0
