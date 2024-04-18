CREATE OR REPLACE TABLE `case-boti-420516.boticario.con_lin_ano_mes`
AS
select ID_LINHA, LINHA, ANO,  MES, SUM_QTD_VENDA
from (select ID_LINHA, LINHA, EXTRACT(year FROM(DATA_VENDA)) AS ANO,  EXTRACT(month FROM(DATA_VENDA)) as MES, SUM(QTD_VENDA) as SUM_QTD_VENDA
from `case-boti-420516.boticario.bases_vendas`
Group by ID_LINHA, LINHA, ANO, MES
order by ano, mes);


CREATE or REPLACE PROCEDURE boticario.proc_con_lin_ano_mes()
  OPTIONS(strict_mode=false)
  BEGIN
    
    MERGE `case-boti-420516.boticario.con_lin_ano_mes` AS target
    USING (select ID_LINHA, LINHA, EXTRACT(year FROM(DATA_VENDA)) AS ANO,  EXTRACT(month FROM(DATA_VENDA)) as MES, SUM(QTD_VENDA) as SUM_QTD_VENDA
    from `case-boti-420516.boticario.bases_vendas`
    Group by ID_LINHA, LINHA, ANO, MES
    order by ano, mes) AS source
        ON target.ID_LINHA = source.ID_LINHA
        and target.LINHA = source.LINHA
        and target.ANO = source.ANO
        and target.MES = source.MES
        and target.SUM_QTD_VENDA = source.SUM_QTD_VENDA
        WHEN MATCHED THEN
          UPDATE SET target.ID_LINHA = source.ID_LINHA,
                    target.LINHA = source.LINHA,
                    target.ANO = source.ANO,
                    target.MES = source.MES,
                    target.SUM_QTD_VENDA = source.SUM_QTD_VENDA
        WHEN NOT MATCHED THEN
          INSERT (ID_LINHA, LINHA, ANO, MES, SUM_QTD_VENDA)
          VALUES (source.ID_LINHA, source.LINHA, source.ANO, source.MES, source.SUM_QTD_VENDA);
      
  END;