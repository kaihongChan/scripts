#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

lock_file="/tmp/order_`date '+%Y%m%d%H'`.lock"
if [ -f "${lock_file}" ];then
    exit 1
fi
touch "${lock_file}"
trap "[ -f \"${lock_file}\" ] && rm -f ${lock_file}" HUP INT QUIT TSTP EXIT

# 每分钟执行
pushd /app/scripts/order
    python3 order.py &
    python3 order_overview.py &
    python3 order_item_overview.py &
    python3 order_item_current.py
popd
wait

exit 0