-- THINGS TO NOTE ABOUT THE STORED PROCEDURE
-- 1. THIS STORED PROCEDURE IS FOR SAGE V7 ONLY.
-- 2. YOU MAY HAVE TO CHANGE THE UOM TO PICK THE CUSTOMER'S APPROPRIATE EFRIS UOM
CREATE PROCEDURE [dbo].[invoiceURA] AS
INSERT INTO
    [_cplURAInv] (
        [orderid],
        [custtinno],
        [custninbrn],
        [custpassportnum],
        [custlegalname],
        [custbusinessname],
        [custaddress],
        [custemail],
        [custmobilephone],
        [custlinephone],
        [custplace],
        [custtype],
        [custcitizenship],
        [custsector],
        [sentstatus],
        [sentlastupdate],
        [sentremarks],
        [sentsignature],
        [docno],
        taxcategory,
        netamount,
        taxrate,
        taxamount,
        grossamount,
        customertype,
        docname
    )
SELECT
    par.autoidx AS orderid,
    cl.Tax_Number AS custtinno,
    '' AS custninbrn,
    '' AS custpassportnum,
    cl.name AS custlegalname,
    cl.name AS custbusinessname,
    cl.post1 + ' ' + cl.post2 + ' ' + cl.post3 AS custaddress,
    cl.email AS custemail,
    cl.Telephone2 AS custmobilephone,
    cl.Telephone AS custlinephone,
    cl.post4 AS custplace,
    (
        CASE
            WHEN cl.ulARCutomerType = 'B2B' THEN '0'
            ELSE '1'
        END
    ) AS custtype,
    '1' AS custcitizenship,
    '' AS custsector,
    'Pending' AS sentstatus,
    NULL AS sentlastupdate,
    '' AS sentremarks,
    '' AS sentsignature,
    par.Reference AS docno,
    'Standard' AS taxcategory,
    ROUND((par.debit - par.credit - par.tax_amount), 2) AS netamount,
    ROUND(
        par.tax_amount / (par.debit - par.credit - par.tax_amount),
        2
    ) AS taxrate,
    ROUND(par.tax_amount, 2) AS taxamount,
    ROUND((par.debit - par.credit), 2) AS grossamount,
    cl.ulARCutomerType AS cutomertype,
    'INV' AS docname
FROM
    postar par
    JOIN client cl ON par.accountlink = cl.dclink
WHERE
    id IN ('OInv', 'IN', 'Inv')
    AND autoidx NOT IN (
        SELECT
            orderid
        FROM
            [_cplURAInv]
    )
    AND par.TxDate >= '2021-10-28'
INSERT INTO
    [_cplUraInvLines] (
        [orderlineid],
        [orderid],
        [Row_Number],
        [itemname],
        [itemcode],
        [itemqty],
        [itemuom],
        [itemprice],
        [linetotprice],
        [taxrate],
        [discount],
        [discounttaxrate],
        [invnumber],
        [discountflag],
        [deemedflag],
        [exciseflag],
        [categoryid],
        [categoryname],
        [goodscategoryid],
        [goodscategoryname],
        [taxamount]
    )
SELECT
    pst.autoidx AS [orderlineid],
    par.autoidx AS [orderid],
    DENSE_RANK() OVER (
        PARTITION BY par.reference
        ORDER BY
            pst.autoidx
    ) - 1 AS [Row_Number],
    stk.description_1 AS [itemname],
    stk.code AS [itemcode],
    pst.[Quantity] AS [itemqty],
    'PCE' AS [itemuom],
    ROUND(
        (pst.credit - pst.debit + pst.Tax_Amount) / [Quantity],
        8
    ) AS [itemprice],
    (pst.credit - pst.debit + pst.Tax_Amount) AS [linetotprice],
    ROUND(pst.[Tax_Amount] / (pst.credit - pst.debit), 2) AS [taxrate],
    0 AS [discount],
    0 AS [discounttaxrate],
    par.reference AS [invnumber],
    2 AS [discountflag],
    2 AS [deemedflag],
    2 AS [exciseflag],
    '' AS [categoryid],
    '' AS [categoryname],
    stk.ucIIURACode AS [goodscategoryid],
    '' AS [goodscategoryname],
    ROUND(pst.Tax_Amount, 2) AS taxamount
FROM
    postst pst
    JOIN postar par ON par.Reference = pst.Reference
    JOIN stkitem stk ON pst.AccountLink = stk.stocklink
WHERE
    par.id IN ('OInv', 'IN', 'Inv')
    AND pst.autoidx NOT IN (
        SELECT
            orderlineid
        FROM
            _cplURAInvLines
    )
    AND par.TxDate >= '2021-10-27'
GO