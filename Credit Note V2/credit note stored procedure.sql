
CREATE procedure [dbo].[CreditNotesURA] as
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
        oriInvoiceId,
        oriInvoiceNo,
        crnreason,
        doctype,
		docname
    )
select
    par.autoidx as orderid,
    cl.registration as custtinno,
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
            when cl.ulARCutomerType = 'B2C' then '2'
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
    round((par.credit - par.debit - par.tax_amount)*-1,2) as netamount,
    round(
        par.tax_amount /(par.credit - par.debit - par.tax_amount),
        2
    ) as taxrate,
    round(par.tax_amount, 2)*-1 as taxamount,
    round((par.credit - par.debit), 2)*-1 as grossamount,
    cl.ulARCutomerType as cutomertype,
    inv.ucIDSOrdUraInvoiceID as oriInvoiceId ,
    inv.ucIDSOrdUraInvoiceNo as oriInvoiceNo,
    (case when crn.ulIDCrnReasonCode = 'miscalculation of price, tax, or discounts' then '103'
		when crn.ulIDCrnReasonCode = 'expiry or damage' then '101'
		when crn.ulIDCrnReasonCode = 'Cancellation of the purchase' then '102' end ) as crnreason,
    'AR' as DocType,
	'CN' as docname
from
    postar par
    join client cl on par.accountlink = cl.dclink
    join invnum inv on inv.invnumber = par.creference2
    join invnum crn on crn.autoindex = par.invnumkey
where
    par.id in ('Crn')
    and par.autoidx not in (
        select
            orderid
        from
            [_cplURAInv]
        where
            DocType = 'AR'
    )
    AND par.TxDate >= '2021-10-28' and inv.ucIDSOrdURAinvoiceNO <> ''
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
        [taxamount],
        DocType,
		creditOriginalRow,
		creference2
    )
select
    pst.autoidx as [orderlineid],
    par.autoidx as [orderid],
    stk.description_1 as [itemname],
    stk.code as [itemcode],
    pst.[Quantity]*-1 as [itemqty],
    'PCE' as [itemuom],
    round(
        (pst.debit - pst.credit + pst.Tax_Amount) / [Quantity],
        2
    ) as [itemprice],
    round((pst.debit - pst.credit + pst.Tax_Amount), 2)*-1 as [linetotprice],
    round(pst.[Tax_Amount] /(pst.debit - pst.credit), 2) as [taxrate],
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
    round(pst.Tax_Amount, 2)*-1 as [taxamount],
    'AR' as DocType,
	0 as creditOriginalRow,
	pst.cReference2 as creference2
from
    postst pst
    join postar par on par.Reference = pst.Reference
    join stkitem stk on pst.AccountLink = stk.stocklink
where
    par.id in ('Crn')
    and pst.autoidx not in (
        select
            orderlineid
        from
            _cplURAInvLines
        where
            DocType = 'AR'
    )
    AND par.TxDate >= '2021-10-28'

GO

