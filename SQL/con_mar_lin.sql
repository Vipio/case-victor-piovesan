CREATE OR REPLACE TABLE `case-boti-420516.boticario.con_mar_lin`
AS
select ID_MARCA, MARCA, ID_LINHA, LINHA, SUM_QTD_VENDA from(
select ID_MARCA, MARCA, ID_LINHA, LINHA, SUM(QTD_VENDA) as SUM_QTD_VENDA
from `case-boti-420516.boticario.bases_vendas`
Group by ID_MARCA, MARCA, ID_LINHA, LINHA
order by ID_MARCA, ID_LINHA);

CREATE or REPLACE PROCEDURE boticario.proc_con_mar_lin()
  OPTIONS(strict_mode=false)
  BEGIN
    
    MERGE `case-boti-420516.boticario.con_mar_lin` AS target
    USING (select ID_MARCA, MARCA, ID_LINHA, LINHA, SUM(QTD_VENDA) as SUM_QTD_VENDA
from `case-boti-420516.boticario.bases_vendas`
Group by ID_MARCA, MARCA, ID_LINHA, LINHA) AS source
        ON target.ID_MARCA = source.ID_MARCA
        and target.MARCA = source.MARCA
        and target.ID_LINHA = source.ID_LINHA
        and target.LINHA = source.LINHA
        and target.SUM_QTD_VENDA = source.SUM_QTD_VENDA
        WHEN MATCHED THEN
          UPDATE SET target.ID_MARCA = source.ID_MARCA,
                    target.MARCA = source.MARCA,
                    target.ID_LINHA = source.ID_LINHA,
                    target.LINHA = source.LINHA,
                    target.SUM_QTD_VENDA = source.SUM_QTD_VENDA
        WHEN NOT MATCHED THEN
          INSERT (ID_MARCA, MARCA, ID_LINHA, LINHA, SUM_QTD_VENDA)
          VALUES (source.ID_MARCA, source.MARCA, source.ID_LINHA, source.LINHA, source.SUM_QTD_VENDA);
      
  END;
