#!/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

lock_file="/tmp/base_`date '+%Y%m%d%H'`.lock"
if [ -f "${lock_file}" ];then
    exit 1
fi
touch "${lock_file}"
trap "[ -f \"${lock_file}\" ] && rm -f ${lock_file}" HUP INT QUIT TSTP EXIT

# 基础数据，每月执行一次
pushd /app/scripts/base
    python3 deliver_place.py &
    python3 traffic_meta.py
popd

wait

exit 0