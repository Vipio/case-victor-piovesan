CREATE OR REPLACE TABLE `case-boti-420516.boticario.con_ano_mes`
AS 
select ANO, MES, SUM_QTD_VENDA FROM (
select EXTRACT(year FROM(DATA_VENDA)) AS ANO,  EXTRACT(month FROM(DATA_VENDA)) as MES, SUM(QTD_VENDA) as SUM_QTD_VENDA
from `case-boti-420516.boticario.bases_vendas`
Group by ANO, MES
order by ano, mes);


CREATE or REPLACE PROCEDURE boticario.proc_con_ano_mes()
  OPTIONS(strict_mode=false)
  BEGIN
    
    MERGE `case-boti-420516.boticario.con_ano_mes` AS target
    USING (select EXTRACT(year FROM(DATA_VENDA)) AS ANO,  EXTRACT(month FROM(DATA_VENDA)) as MES, SUM(QTD_VENDA) as SUM_QTD_VENDA
from `case-boti-420516.boticario.bases_vendas`
Group by ANO, MES) AS source
        ON target.ANO = source.ANO
        and target.MES = source.MES
        and target.SUM_QTD_VENDA = source.SUM_QTD_VENDA
        WHEN MATCHED THEN
          UPDATE SET target.ANO = source.ANO,
                    target.MES = source.MES,
                    target.SUM_QTD_VENDA = source.SUM_QTD_VENDA
        WHEN NOT MATCHED THEN
          INSERT (ANO, MES, SUM_QTD_VENDA)
          VALUES (source.ANO, source.MES, source.SUM_QTD_VENDA);
      
  END;