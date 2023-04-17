## 定时任务设置（crontab）
```
@monthly /app/script/common/base.sh >> /app/logs/common.log
@monthly /app/script/shop/shop_info_job.sh >> /app/logs/shop_info.log

0 9 * * * /app/script/shop/shop_job.sh >> /app/logs/shop.log
0 9 * * * /app/script/ware/item_job.sh >> /app/logs/item.log

0 */3 * * * /app/script/order/order_job.sh >> /app/logs/order.log
0 */3 * * * /app/script/shipment/shipment_job.sh >> /app/logs/shipment.log
0 */3 * * * /app/script/refund/refund_job.sh >> /app/logs/refund.log

* * * * * /app/script/fordeal.sh >> /app/logs/fordeal.log
* * * * * /app/script/refund/refund.sh >> /app/logs/refund.log
* * * * * /app/script/shop/shop_info.sh >> /app/logs/shop_info.log
* * * * * /app/script/shop/shop.sh >> /app/logs/shop.log
* * * * * /app/script/order/order.sh >> /app/logs/order.log
* * * * * /app/script/ware/item.sh >> /app/logs/wares.log
* * * * * /app/script/shipment/shipment.sh >> /app/logs/shipment.log
```

