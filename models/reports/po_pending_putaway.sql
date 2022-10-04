SELECT co.customerid,ph.custowner,ph.warehouse,ph.pono AS pono,IFNULL(ph.potype,'') AS potype,vm.company AS vendor,
       ph.orderdate,ph.arriveddatetime,MAX(id.recvdate) AS last_recvdate,COUNT(DISTINCT id.itemno) AS items, 
       SUM(id.qty) AS total_units, current_timestamp AS last_updated_ts 
FROM {{ ref('unified_itemdetail') }}  id
JOIN {{ ref('unified_containers') }} c ON c.containerid=id.container AND c.ordertype='PO' AND c.warehouse=id.warehouse
AND id.VALID_TO is null AND c.VALID_TO is null
JOIN {{ ref('unified_poheader') }} ph ON ph.pono=c.orderno AND ph.custowner=id.custowner 
AND ph.warehouse=id.warehouse AND ph.VALID_TO is null
JOIN {{ ref('unified_vendmaster') }} vm ON vm.custowner=ph.custowner AND vm.vendid=ph.vendorid 
AND vm.warehouse=ph.warehouse AND vm.VALID_TO is null
JOIN {{ ref('unified_custowners') }} co ON ph.custowner = co.custowner AND ph.warehouse=co.warehouse AND co.VALID_TO is null
WHERE id.bin IN ('RECV','RSTG','XDOCK')
AND id.status IN ('Avail', 'RECEIVED')
GROUP BY ph.warehouse,ph.custowner,co.customerid,ph.pono,ph.potype,vm.company,ph.orderdate,ph.arriveddatetime
ORDER BY ph.arriveddatetime