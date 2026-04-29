     INDEXID INDEXMODULECODE         INDEXNAME
0  379978898              12  MSCI World Index

Q:
1. currently i am doing calculation in sql, should we forbid that (toetal return, e.g.)
     A: no.. too confusing to have the calculation there
2. for index, tradingitemtype can take 1 (gross return),2 (price return) or 4 (net return). This mapping is not given in the database per se, but only on CIQ-website. Where should we put it as reference? (direct comment, create a simple .csv, create a database table...)
     A: inline documents... not super strict...  just comment in schema for example
3. to generalize on 2., where do we put the id -> name mapping tables storing the ones we need?
     A: no mapping! to save space
5. Following the logic of CV variable, do we strictly avoid in one sql, query several data table. (for example, getting some fundamnetals data and estimates data in one query)
     A: no we never need those.
6. should we always have a parameter, "peek" to insert "limit 10" in the end, for data preview? 
     A: oepn an issue.. to decide later (might not unnecessary)
7. for price and sometimes other values, the precision is very high, should we in sql already drop the precision to .xxxx for example, in order to shrink size? 
     A: talk about later when we meet the problem
8. standard naming. companyid, or company_id? where to convert? in sql, or afterwards in python?
     A: we do not convert 
9. default value, in sql or python passes in. Add limit 10...
     A: we do not use jinja default, we do not use jinja if-condition

Remarks:
1. pydantic input and output check