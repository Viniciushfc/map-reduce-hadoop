# MovieAnalysisNetflixCsv

Este projeto implementa uma análise de descrições de filmes e séries da Netflix usando **Hadoop MapReduce** em Java. Ele processa um dataset CSV contendo títulos e descrições e gera diversas métricas relevantes.

---

## Funcionalidades

O programa realiza as seguintes análises:

1. **Contagem de palavras** em cada descrição.
2. **Título com mais palavras** e **título com menos palavras**.
3. **Número total de palavras** no dataset.
4. **Média de palavras por descrição**.
5. **Top 5 palavras mais frequentes** (ignora stopwords).
6. **Top 5 palavras menos frequentes** (ignora stopwords).
7. **Normalização do texto**:
   - Conversão para letras minúsculas
   - Remoção de pontuação e caracteres especiais
   - Stopwords ignoradas (como "de", "em", "para", etc.)

---

## Estrutura do projeto

```
MovieAnalysisNetflixCsv/
│
├─ src/main/java/br/com/viniciushfc/
│  └─ MovieAnalysisNetflixCsv.java
│
├─ input/              # Arquivos de entrada CSV
└─ output/             # Diretório para resultados do Hadoop
```

O CSV de entrada deve ter o formato:

```
Título,Descrição
Stranger Things,Uma série de suspense com elementos sobrenaturais...
Breaking Bad,Um professor de química se transforma em criminoso...
...
```

---

## Pré-requisitos

- Java 8 ou superior
- Hadoop 2.x ou superior
- HDFS configurado
- Maven (opcional, para build)

---

## Como compilar e executar

### 1. Compilar o código

Se estiver usando Maven:

```bash
mvn clean package
```

Isso gera o arquivo `MovieAnalysisNetflixCsv.jar` na pasta `target/`.

### 2. Copiar os arquivos para o HDFS

```bash
hdfs mkdir -p /user/<seu_usuario>/netflix/input
hdfs put input/*.csv /user/<seu_usuario>/netflix/input/
```

### 3. Executar o job Hadoop

```bash
hadoop jar target/MovieAnalysisNetflixCsv.jar br.com.viniciushfc.MovieAnalysisNetflixCsv \
  /user/<seu_usuario>/netflix/input/ \
  /user/<seu_usuario>/netflix/output/resultado
```

### 4. Visualizar os resultados

```bash
hdfs ls /user/<seu_usuario>/netflix/output/resultado
hdfs cat /user/<seu_usuario>/netflix/output/resultado/part-r-00000
```

**Exemplo de saída:**

```
Título com mais palavras: Stranger Things   152
Título com menos palavras: Mini Movie       6
Número total de palavras:                   1035614
Média de palavras por descrição:           117

Top 5 palavras mais frequentes:
tv     15366
the    10451
to     6707
and    6607
movie  6245

Top 5 palavras menos frequentes:
zz     1
zynnell 2
zylka  3
zyatan 4
```

---

## Desafio

- O Mapper faz normalização do texto e remove stopwords.
- O Reducer calcula todas as métricas globais no final usando o método `cleanup()`.
- O projeto pode ser facilmente adaptado para outros datasets CSV de filmes ou séries.
- Para grandes datasets, o Hadoop processa os dados de forma distribuída, garantindo escalabilidade.

---

## Desafio extra implementado

- Ignora stopwords no cálculo das palavras mais frequentes.
- Calcula a média de palavras por descrição.

## Licença

Este projeto está sob a licença MIT.
