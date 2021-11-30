
CREATE PROCEDURE [dbo].[EFRIS-STOCK]
AS
  INSERT INTO dbo._CplUraStocks (id
  , quantity
  , unitcost
  , stockcode
  , stockname
  , operationType
  , batchNumber
  , batchDate
  , adjustType
  , UoM
  , stockIntype
  , URAstatus
  , URAResponse)
    SELECT
      pst.autoidx AS id
     ,pst.quantity AS quantity
     ,CASE
        WHEN pst.debit > 0 THEN ROUND(pst.debit / pst.quantity, 2)
        ELSE ROUND(pst.credit / quantity, 2)
      END AS unitcost
     ,REPLACE(stk.code, ' ', '') AS stockcode
     ,stk.description_1 AS stockname
     ,CASE
        WHEN debit > 0 THEN '101'
        ELSE '102'
      END AS operationtype
     ,CAST(pst.cAuditNumber AS NVARCHAR) + '-' + CAST(pst.AutoIdx AS NVARCHAR) + '-' + CAST(pst.cReference2 AS NVARCHAR) AS batchnumber
     ,pst.TxDate AS batchdate
     ,CASE
        WHEN credit > 0 THEN '105'
        ELSE ''
      END
      AS adjustType
     ,(CASE
        WHEN stk.iUOMStockingUnitID = 4 THEN 'SW'
        WHEN stk.iUOMStockingUnitID = 6 THEN '103'
        WHEN stk.iUOMStockingUnitID = 2 THEN 'PCE'
      END) AS UoM
      --,'101' AS UoM
     ,CASE
        WHEN debit > 0 THEN '103'
        ELSE ''
      END AS stockintype
     ,'PENDING' AS urastatus
     ,'PENDING' AS URAResponse
    FROM PostST pst
    JOIN StkItem stk
      ON pst.AccountLink = stk.StockLink
    WHERE pst.id IN ('Ogrv', 'grv', 'ijr', 'Sadj', 'rts')
    AND pst.Quantity > 0
    AND pst.autoidx NOT IN (SELECT
        id
      FROM _CplUraStocks)
    AND pst.txdate >= '2021-11-01'
