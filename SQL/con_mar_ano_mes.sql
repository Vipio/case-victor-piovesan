CREATE OR REPLACE TABLE `case-boti-420516.boticario.con_mar_ano_mes`
AS
select ID_MARCA, MARCA, ANO,  MES, SUM_QTD_VENDA
from (select ID_MARCA, MARCA,EXTRACT(year FROM(DATA_VENDA)) AS ANO,  EXTRACT(month FROM(DATA_VENDA)) as MES, SUM(QTD_VENDA) as SUM_QTD_VENDA from `case-boti-420516.boticario.bases_vendas`
Group by ID_MARCA, MARCA, ANO, MES
order by ano, mes);


CREATE or REPLACE PROCEDURE boticario.proc_con_mar_ano_mes()
  OPTIONS(strict_mode=false)
  BEGIN
    
    MERGE `case-boti-420516.boticario.con_mar_ano_mes` AS target
    USING (select ID_MARCA, MARCA,EXTRACT(year FROM(DATA_VENDA)) AS ANO,  EXTRACT(month FROM(DATA_VENDA)) as MES, SUM(QTD_VENDA) as SUM_QTD_VENDA from `case-boti-420516.boticario.bases_vendas`
Group by ID_MARCA, MARCA, ANO, MES) AS source
        ON target.ID_MARCA = source.ID_MARCA
        and target.MARCA = source.MARCA
        and target.ANO = source.ANO
        and target.MES = source.MES
        and target.SUM_QTD_VENDA = source.SUM_QTD_VENDA
        WHEN MATCHED THEN
          UPDATE SET target.ID_MARCA = source.ID_MARCA,
                    target.MARCA = source.MARCA,
                    target.ANO = source.ANO,
                    target.MES = source.MES,
                    target.SUM_QTD_VENDA = source.SUM_QTD_VENDA
        WHEN NOT MATCHED THEN
          INSERT (ID_MARCA, MARCA, ANO, MES, SUM_QTD_VENDA)
          VALUES (source.ID_MARCA, source.MARCA, source.ANO, source.MES, source.SUM_QTD_VENDA);
      
  END;