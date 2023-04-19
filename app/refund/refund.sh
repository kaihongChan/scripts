#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

lock_file="/tmp/refund_`date '+%Y%m%d%H'`.lock"
if [ -f "${lock_file}" ];then
    exit 1
fi
touch "${lock_file}"
trap "[ -f \"${lock_file}\" ] && rm -f ${lock_file}" HUP INT QUIT TSTP EXIT

pushd /app/scripts/refund
    python3 refund.py &
    python3 es_img.py
popd
wait

exit 0