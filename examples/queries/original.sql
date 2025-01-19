WITH base_inventory AS (
    SELECT i.*
    FROM invtory i
    WHERE i.partno LIKE '%DM%'
),
inventory_agg AS (
    SELECT 
        i.make,
        i.partno,
        i.office,
        i.invtype,
        i.selluom,
        SUM(i.onhandqty) as total_onhand,
        SUM(i.shelfqty) as total_shelf,
        SUM(i.secondaryshelfqty) as total_secondary,
        SUM(i.tertiaryshelfqty) as total_tertiary,
        SUM(i.fourthlyshelfqty) as total_fourthly,
        SUM(CASE WHEN i.secondaryshelfdisabled THEN i.secondaryshelfqty * COALESCE(i.eaqty, 1) ELSE 0 END) as secondary_disabled,
        SUM(CASE WHEN i.tertiaryshelfdisabled THEN i.tertiaryshelfqty * COALESCE(i.eaqty, 1) ELSE 0 END) as tertiary_disabled,
        SUM(CASE WHEN i.fourthlyshelfdisabled THEN i.fourthlyshelfqty * COALESCE(i.eaqty, 1) ELSE 0 END) as fourthly_disabled
    FROM base_inventory i
    GROUP BY i.make, i.partno, i.office, i.invtype, i.selluom
)
SELECT DISTINCT 
    t.make AS title_make,
    t.partno AS title_partno,
    t.inactive AS title_inactive,
    t.description AS title_description,
    t.nsr AS title_nsr,
    i.invtype AS invtory_invtype,
    i.selluom AS invtory_selluom,
    i.total_onhand AS invtory_qty,
    COALESCE(i.total_shelf, 0) AS shelfqty,
    COALESCE(i.total_secondary, 0) AS secondaryshelfqty,
    COALESCE(i.total_tertiary, 0) AS tertiaryshelfqty,
    COALESCE(i.total_fourthly, 0) AS fourthlyshelfqty,
    COALESCE(i.secondary_disabled, 0) AS secondaryshelfdisabledqty,
    COALESCE(i.tertiary_disabled, 0) AS tertiaryshelfdisabledqty,
    COALESCE(i.fourthly_disabled, 0) AS fourthlyshelfdisabledqty,
    CASE 
        WHEN i.selluom = q.selluom THEN q.onhandqty
        ELSE q.onhandqty / COALESCE((
            SELECT ratio 
            FROM titleconversion tc 
            WHERE tc.make = t.make 
            AND tc.partno = t.partno 
            AND tc.selluom = i.selluom 
            AND tc.invqtyuom = q.selluom
            LIMIT 1
        ), 1)
    END AS invqty_onhandqty,
    CASE 
        WHEN i.selluom = q.selluom THEN q.committedqty
        ELSE q.committedqty / COALESCE((
            SELECT ratio 
            FROM titleconversion tc 
            WHERE tc.make = t.make 
            AND tc.partno = t.partno 
            AND tc.selluom = i.selluom 
            AND tc.invqtyuom = q.selluom
            LIMIT 1
        ), 1)
    END AS invqty_committedqty,
    CASE 
        WHEN i.selluom = q.selluom THEN q.backorder
        ELSE q.backorder / COALESCE((
            SELECT ratio 
            FROM titleconversion tc 
            WHERE tc.make = t.make 
            AND tc.partno = t.partno 
            AND tc.selluom = i.selluom 
            AND tc.invqtyuom = q.selluom
            LIMIT 1
        ), 1)
    END AS invqty_backorder,
    q.selluom AS invqty_selluom,
    i.office
FROM inventory_agg i
INNER JOIN title t ON 
    t.make = i.make AND 
    t.partno = i.partno
INNER JOIN invqty q ON 
    q.make = i.make AND 
    q.partno = i.partno AND 
    q.office = i.office
WHERE POSITION(i.selluom IN COALESCE(t.hideuom, '')) <= 0
ORDER BY 
    title_make,
    title_partno,
    invtory_invtype,
    invtory_qty;
