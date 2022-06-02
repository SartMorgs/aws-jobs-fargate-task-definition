# Transformação de Json

Esse job tem como objetivo ler dados de um json dinâmico e aninhado, realizar transformações nos dados visando desaninhar os dados e transformá-los em formato split.

## Dependências

- Imagem docker com pyspark: [godatadriven/pyspark](https://hub.docker.com/r/godatadriven/pyspark)
- Repositório de arquivo de configuração: [cfg-files](https://bitbucket.org/mobicare-git/cfg-files/src/master/)

## Fluxo da Rotina

![](https://bitbucket.org/mobicare-git/ecr-images-repository/src/master/de-extract-data/bi-json-transform/img/flow.PNG)

O job consiste de ler um json fonte e um arquivo de configuração de esquema. Nesse arquivo de configuração de esquema é descrito as colunas que são header do arquivo, nas quais será feito o primeiro filtro e expansão de suas colunas internas, e quais colunas terão seus campos explodidos. 
O arquivo de configuração de esquema segue este modelo:

```yaml
schema:
  headers:
    - column1
    - column2
    - column3
  explode:
    - column2
  inside_fields:
    column1:
      - subcolumn1
      - subcolumn2
      - subcolumn3
    column3:
      - subcolumn1
      - subcolumn2
      - subcolumn3
      - subcolumn4
```

O resultado será um json com configuração split de suas colunas.

## Referências

[How to create a Docker Container with Pyspark ready to work with Elyra](https://ruslanmv.com/blog/Docker-Container-with-Pyspark-and-Jupyter-and-Elyra)