CREATE TABLE case-boti-420516.boticario.bases_vendas (
ID_MARCA	INTEGER,		
MARCA	    STRING,
ID_LINHA	INTEGER,		
LINHA	    STRING,	
DATA_VENDA	TIMESTAMP,		
QTD_VENDA	INTEGER,
);


CREATE OR REPLACE PROCEDURE boticario.proc_bases_vendas()
  OPTIONS(strict_mode=false)
  BEGIN
    CREATE OR REPLACE TABLE `case-boti-420516.boticario.bases_vendas`
    AS
      SELECT ID_MARCA, MARCA, ID_LINHA, LINHA, DATA_VENDA, QTD_VENDA
      FROM (
        SELECT ID_MARCA, MARCA, ID_LINHA, LINHA, DATA_VENDA, QTD_VENDA,
              ROW_NUMBER() OVER (PARTITION BY ID_MARCA, MARCA, ID_LINHA, LINHA, DATA_VENDA, QTD_VENDA ORDER BY ID_MARCA, MARCA, ID_LINHA, LINHA, DATA_VENDA, QTD_VENDA) AS row_number
        FROM `case-boti-420516.boticario.bases_vendas`
    )
    WHERE row_number = 1;
  END;