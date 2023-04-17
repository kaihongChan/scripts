#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH
export PATH

lock_file="/app/scripts/tmp/shop_handlers.lock"
if [ -f "${lock_file}" ];then
    exit 1
fi
touch "${lock_file}"
#trap "[ -f \"${lock_file}\" ] && rm -f ${lock_file}" HUP INT QUIT TSTP EXIT

pushd /app/scripts/base
    python3 deliver_place_exec.py &
    python3 traffic_meta_exec.py
popd

pushd /app/scripts/shipment
    python3 inbound_plan.py &
    python3 shipment.py
popd

pushd /app/scripts/refund
    python3 refund.py &
    python3 es_img.py
popd

pushd /app/scripts/order
    python3 order.py &
    python3 order_overview.py &
    python3 order_item_overview.py &
    python3 order_item_current.py
popd

pushd /app/scripts/shop
    python3 shop_traffic.py &
    python3 shop_trans.py &
    python3 shop_trend.py &
    python3 shop_flow_source.py
popd

pushd /app/scripts/ware
    python3 item_flow_rank.py &
    python3 items_indicate.py &
    python3 item_overview.py &
    python3 item_traffic.py &
    python3 item_trend.py
popd

wait
pushd /app/scripts
    python3 set_finish.py
popd

wait
exit 0