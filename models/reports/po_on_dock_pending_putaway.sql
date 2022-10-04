WITH PENDING_CONTAINERS AS (
SELECT ph.warehouse,ph.custowner, ph.pono, IFNULL(ph.potype,'') AS potype, vm.vendid AS vendor, ph.orderdate,
       MAX(id.recvdate) AS last_recvdate, COUNT(DISTINCT id.container) AS containers, SUM(id.qty) AS units
FROM {{ ref('unified_itemdetail') }} id
JOIN {{ ref('unified_containers') }} c ON c.containerId = id.container AND c.ordertype = 'PO' AND c.warehouse=id.warehouse
AND id.VALID_TO is null AND c.VALID_TO is null
JOIN {{ ref('unified_poheader') }} ph ON ph.pono = c.orderno AND ph.custowner = id.custowner
AND ph.warehouse=id.warehouse AND ph.VALID_TO is null
JOIN {{ ref('unified_vendmaster') }} vm ON vm.custowner = ph.custowner AND vm.vendid = ph.vendorid
AND vm.warehouse=ph.warehouse AND vm.VALID_TO is null
WHERE id.bin IN ('RECV','RSTG','XDOCK') AND id.status IN ('Avail', 'RECEIVED') 
GROUP BY ph.warehouse,ph.custowner, ph.pono, ph.potype, vm.vendid, ph.orderdate
)
SELECT co.customerid,ph.custowner,ph.warehouse,ph.pono,ph.altpono AS altpono,ph.status,ph.orderdate,ph.arriveddatetime,
      vm.company AS vendor,SUM(pd.orderquantity) AS order_qty,SUM(pd.recvquantity) AS recv_qty,COUNT(DISTINCT pd.itemno) AS items,
      IFNULL(pc.containers, 0) AS pending_containers,IfNULL(pc.units, 0) AS pending_units,current_timestamp AS last_updated_ts
FROM {{ ref('unified_poheader') }} ph
JOIN {{ ref('unified_podetail') }} pd ON pd.custowner=ph.custowner AND pd.pono=ph.pono AND pd.warehouse=ph.warehouse
AND ph.VALID_TO is null AND pd.VALID_TO is null
JOIN {{ ref('unified_vendmaster') }} vm ON vm.custowner=ph.custowner AND vm.vendid = ph.vendorid 
AND vm.warehouse=ph.warehouse AND vm.VALID_TO is null
JOIN {{ ref('unified_custowners') }} co ON co.custowner = ph.custowner AND co.enabled = 'TRUE' 
AND co.warehouse=ph.warehouse AND co.VALID_TO is null
LEFT JOIN PENDING_CONTAINERS pc
ON pc.warehouse=ph.warehouse AND pc.custowner = ph.custowner AND pc.pono = ph.pono AND pc.vendor = ph.vendorid
WHERE ph.status NOT IN ('Closed','Cancelled') AND ph.arrived = 'TRUE' AND ph.completed = 'FALSE'
GROUP BY ph.warehouse,ph.custowner, co.customerid, ph.pono, ph.altpono, ph.status,
         ph.orderdate, ph.arriveddatetime, vm.company, pc.containers, pc.units, ph.vendorid