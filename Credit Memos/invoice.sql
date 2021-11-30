
    ALTER procedure [dbo].[invoiceURA] as
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
select
    par.autoidx as orderid,
    cl.Tax_Number as custtinno,
    '' as custninbrn,
    '' as custpassportnum,
    cl.name as custlegalname,
    cl.name as custbusinessname,
    cl.post1 + ' ' + cl.post2 + ' ' + cl.post3 as custaddress,
    cl.email as custemail,
    cl.Telephone2 as custmobilephone,
    cl.Telephone as custlinephone,
    cl.post4 as custplace,
    (
        case
            when cl.ulARCutomerType = 'B2B' then '0'
            else '1'
        end
    ) as custtype,
    '1' as custcitizenship,
    '' as custsector,
    'Pending' as sentstatus,
    null as sentlastupdate,
    '' as sentremarks,
    '' as sentsignature,
    par.Reference as docno,
    'Standard' as taxcategory,
    round((par.debit - par.credit - par.tax_amount), 2) as netamount,
    round(
        par.tax_amount /(par.debit - par.credit - par.tax_amount),
        2
    ) as taxrate,
    round(par.tax_amount, 2) as taxamount,
    round((par.debit - par.credit), 2) as grossamount,
    cl.ulARCutomerType as cutomertype,
    'INV' as docname
from
    postar par
    join client cl on par.accountlink = cl.dclink
where
    id in ('OInv', 'IN', 'Inv')
    and autoidx not in (
        select
            orderid
        from
            [_cplURAInv]
    )
    AND par.DTStamp >= '2021-03-05 12:00:00'
INSERT INTO
    [_cplUraInvLines] (
        [orderlineid],
        [orderid],
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
select
    pst.autoidx as [orderlineid],
    par.autoidx as [orderid],
    stk.description_1 as [itemname],
    stk.code as [itemcode],
    pst.[Quantity] as [itemqty],
    '101' as [itemuom],
    round(
        (pst.credit - pst.debit + pst.Tax_Amount) / [Quantity],
        8
    ) as [itemprice],
(pst.credit - pst.debit + pst.Tax_Amount) as [linetotprice],
    round(pst.[Tax_Amount] /(pst.credit - pst.debit), 2) as [taxrate],
    0 as [discount],
    0 as [discounttaxrate],
    par.reference as [invnumber],
    2 as [discountflag],
    2 as [deemedflag],
    2 as [exciseflag],
    '' as [categoryid],
    '' as [categoryname],
    stk.ucIIURACode as [goodscategoryid],
    '' as [goodscategoryname],
    round(pst.Tax_Amount, 2) as taxamount
from
    postst pst
    join postar par on par.Reference = pst.Reference
    join stkitem stk on pst.AccountLink = stk.stocklink
where
    par.id in ('OInv', 'IN', 'Inv')
    and pst.autoidx not in (
        select
            orderlineid
        from
            _cplURAInvLines
    )
    AND par.DTStamp >= '2021-03-05 12:00:00'