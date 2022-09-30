SELECT * FROM
(
SELECT iq.warehouse,iq.custowner,co.customerid,date, ordertype, SUM(pending_units) AS pending_units,SUM(open_units) AS open_units,
       SUM(picking_units) as picking_units,SUM(picked_units) as picked_units,SUM(all_open_units) AS all_open_units,
       SUM(pending_orders) AS pending_orders,SUM(open_orders) AS open_orders,
       SUM(picking_orders) AS picking_orders,SUM(picked_orders)AS picked_orders,
       SUM(all_open_orders) AS all_open_orders,date_type, current_timestamp as backlog_summary_snapshot_date
FROM
    (SELECT  sm.warehouse,sm.custowner,sm.ordertype
		, SUM(CASE WHEN sm.status IN ('Available','Pending') THEN sd.orderqty ELSE 0 END) AS pending_units
		, SUM(CASE WHEN sm.status IN ('Planned','Allocated','Inducted') THEN sd.orderqty ELSE 0 END) AS open_units
		, SUM(CASE WHEN sm.status IN ('Picking') THEN sd.orderqty ELSE 0 END) AS picking_units
		, SUM(CASE WHEN sm.status IN ('Picked') THEN sd.orderqty ELSE 0 END) AS picked_units
		, SUM(CASE WHEN sm.status IN ('Available','Pending','Planned','Allocated','Inducted','Picking','Picked') THEN sd.orderqty ELSE 0 END) AS all_open_units
        
        , CASE WHEN sm.status IN ('Available','Pending') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS pending_orders
		, CASE WHEN sm.status IN ('Planned','Allocated','Inducted') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS open_orders
		, CASE WHEN sm.status IN ('Picking') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS picking_orders
		, CASE WHEN sm.status IN ('Picked') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS picked_orders
		, CASE WHEN sm.status IN ('Available','Pending','Planned','Allocated','Inducted','Picking','Picked') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS all_open_orders
        , TO_DATE(sm.createdatetime) as date, 'CREATE_DATE' AS date_type
    FROM   {{ ref('unified_shipmaster') }} sm, {{ ref('unified_shipdetail') }} sd
    WHERE  sm.status in ('Available','Pending','Planned','Allocated','Inducted','Picking','Picked') 
    AND    sm.sono = sd.sono AND sd.custowner = sm.custowner AND sm.warehouse = sd.warehouse
    GROUP BY sm.warehouse,sm.custowner,sm.ordertype,sm.status, date,date_type
    ORDER BY sm.warehouse,sm.ordertype) iq
    LEFT JOIN {{ ref('unified_custowners') }} co ON co.warehouse = iq.warehouse AND co.custowner = iq.custowner
    GROUP BY iq.warehouse,iq.custowner,co.customerid, ordertype,date,date_type
UNION
SELECT iq.warehouse,iq.custowner,co.customerid, date, ordertype, SUM(pending_units) AS pending_units,SUM(open_units) AS open_units,
       SUM(picking_units) AS picking_units,SUM(picked_units) as picked_units,SUM(all_open_units) AS all_open_units,
       SUM(pending_orders) AS pending_orders,SUM(open_orders) AS open_orders,
       SUM(picking_orders) AS picking_orders,SUM(picked_orders)AS picked_orders,
       SUM(all_open_orders) AS all_open_orders,date_type, current_timestamp AS backlog_summary_snapshot_date
FROM
    (SELECT  sm.warehouse,sm.custowner, sm.ordertype
		, SUM(CASE WHEN sm.status IN ('Available','Pending') THEN sd.orderqty ELSE 0 END) AS pending_units
		, SUM(CASE WHEN sm.status IN ('Planned','Allocated','Inducted') THEN sd.orderqty ELSE 0 END) AS open_units
		, SUM(CASE WHEN sm.status IN ('Picking') THEN sd.orderqty ELSE 0 END) AS picking_units
		, SUM(CASE WHEN sm.status IN ('Picked') THEN sd.orderqty ELSE 0 END) AS picked_units
		, SUM(CASE WHEN sm.status IN ('Available','Pending','Planned','Allocated','Inducted','Picking','Picked') THEN sd.orderqty ELSE 0 END) AS all_open_units
        
        , CASE WHEN sm.status IN ('Available','Pending') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS pending_orders
		, CASE WHEN sm.status IN ('Planned','Allocated','Inducted') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS open_orders
		, CASE WHEN sm.status IN ('Picking') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS picking_orders
		, CASE WHEN sm.status IN ('Picked') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS picked_orders
		, CASE WHEN sm.status IN ('Available','Pending','Planned','Allocated','Inducted','Picking','Picked') THEN COUNT(DISTINCT sm.sono) ELSE 0 END AS all_open_orders
        , TO_DATE(sm.ORDERDATE) AS date, 'ORDERDATE' as date_type
    FROM   {{ ref('unified_shipmaster') }} sm, {{ ref('unified_shipdetail') }} sd
    WHERE  sm.status in ('Available','Pending','Planned','Allocated','Inducted','Picking','Picked') 
    AND    sm.sono = sd.sono AND sd.custowner = sm.custowner AND sm.warehouse = sd.warehouse
    GROUP BY sm.warehouse,sm.custowner,sm.ordertype,sm.status, date,date_type
    ORDER BY sm.warehouse,sm.ordertype) iq
    LEFT JOIN {{ ref('unified_custowners') }} co ON co.warehouse = iq.warehouse AND co.custowner = iq.custowner
    GROUP BY iq.warehouse,iq.custowner,co.customerid,date,ordertype,date_type)
ORDER BY date_type,warehouse,custowner, date DESC,ordertype