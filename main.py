import pymysql
import pymysql.cursors
from typing import cast       # para alterar tipagem do cursor / cast faz conversão de tipo, passando o tipo e o valor para conversão, numa variável qualquer // isso é para quando tem muito # type: ignore.

TABLE_NAME = 'customers'
# CURRENT_CURSOR = pymysql.cursors.SSDictCursor      # >>> SSDictCursor usado para volume enorme de dados / usar DictCursor para dados.
CURRENT_CURSOR = pymysql.cursors.DictCursor


connection = pymysql.connect(
    host='localhost',    
    user='root',
    password='123456',
    #cursorclass=pymysql.cursors.DictCursor
    cursorclass=CURRENT_CURSOR
)

#################################################################
# CRIANDO DATABASE E TABLE

cursor = connection.cursor()    # 1ª forma definindo var cursor

cursor.execute('CREATE DATABASE IF NOT EXISTS db_teste')
connection.select_db('db_teste')

cursor.execute(
    f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} ('
    'id INT NOT NULL AUTO_INCREMENT, '
    'nome VARCHAR(50) NOT NULL, '
    'idade INT NOT NULL, '
    'PRIMARY KEY (id)'
    ')'
)
cursor.execute(f'TRUNCATE TABLE {TABLE_NAME}')
connection.commit()

# TRUNCATE TABLE >>> Cuidado: limpa a tabela. Remove todas as linhas de uma tabela, mas permanecem a estrutura da tabela e suas colunas, restrições, índices etc.

#################################################################

# NÃO É PRUDENTE ENVIAR VALORES JUNTO COM COMANDO SQL NA MESMA CONSULTA COMO DESSA FORMA ABAIXO, ABRINDO BRECHA NA SEGURANÇA DO CÓDIGO. EVITAR SQL INJECTION. ATAQUES EXTERNOS. SQL CODIFICA QUANDO SEPARAM OS DADOS DO COMANDO COM PLACEHOLDERS :
# with connection:    # 2ª forma definindo var cursor
#     with connection.cursor() as cursor:
#         cursor.execute(
#             f'INSERT INTO {TABLE_NAME} '
#             '(nome, idade) VALUES ("Luiz", 25) '
#         )   
               
#         result = cursor.execute(
#             f'INSERT INTO {TABLE_NAME} '
#             '(nome, idade) VALUES ("Paulo", 30) '
#         )
#         print(result)       # nº de linhas afetadas
#     connection.commit()


# Começo a manipular dados a partir daqui

# Inserindo um valor usando placeholder e um iterável
with connection:
    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE_NAME} '
            '(nome, idade) VALUES (%s, %s)'
        )

        data = ('Luiz', 18)
        result = cursor.execute(sql, data)
        print(sql, data)    # opcional mostrando no terminal
        print(result)       # opcional mostrando no terminal
    connection.commit()

# Inserindo um valor usando placeholder e um dicionário
    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE_NAME} '
            '(nome, idade) VALUES (%(name)s, %(age)s)'
        )

        data2 = {"age": 37, "name": "Le"}
        result = cursor.execute(sql, data2)
        print(sql)      # terminal
        print(data2)    # terminal
        print(result)   # terminal
    connection.commit()

    # Inserindo vários valores usando placeholder e uma tupla de dicionários (ou iterável de dicionários)
    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE_NAME} '
            '(nome, idade) VALUES (%(name)s, %(age)s)'
        )
        data3 = (
            {"name": "Sah", "age": 33},
            {"name": "Júlia", "age": 74},
            {"name": "Rose", "age": 53},
        )
        result = cursor.executemany(sql, data3)
        print(sql)
        print(data3)
        print(result)
    connection.commit()

    # Inserindo vários valores usando placeholder e uma tupla de tuplas (ou iterável de iteráveis)
    with connection.cursor() as cursor:
        sql = (
            f'INSERT INTO {TABLE_NAME} '
            '(nome, idade) VALUES (%s, %s)'
        )
        data4 = (
            ('Siri', 22),
            ('Helena', 15),
            ('Luiz', 18)
        )
        result = cursor.executemany(sql, data4)
        print(sql)
        print(data4)
        print(result)
    connection.commit()

    #######################

    # a) Lendo os valores com SELECT
    # with connection.cursor() as cursor:
    #     sql = (
    #         f'SELECT * FROM {TABLE_NAME} '  #SELECT id, nome, idade FROM
    #     )
    #     cursor.execute(sql)
    #     data5 = cursor.fetchall()
    #     # data6 = cursor.fetchone()
    #     # print(data6)
        
    #     for row in data5:
    #         print(row)

    
    # b) Lendo os valores com SELECT e WHERE
    with connection.cursor() as cursor:
        # menor_id = int(input('Digite o menor id: '))
        # maior_id = int(input('Digite o maior id: '))
        menor_id = 2
        maior_id = 4

        sql = (
            f'SELECT * FROM {TABLE_NAME} '
            'WHERE id BETWEEN %s AND %s'   # WHERE id = $s' (apenas id n)
        )   # sempre uar plaholder '%s' para evitar SQL Injection (ataques, ameaças de segurança)

        cursor.execute(sql, (menor_id, maior_id))
        print(cursor.mogrify(sql, (menor_id, maior_id)))
        data7 = cursor.fetchall()

        for row in data7:
            print(row)

    ######################

    # Apagando com DELETE, WHERE e placeholders no PyMySQL
    with connection.cursor() as cursor:
        sql = (
            f'DELETE FROM {TABLE_NAME} '
            'WHERE id = %s'     
        )   # DELETE e UPDATE precisam de WHERE por segurança dos dados
        # print(cursor.execute(sql, (1)))
        cursor.execute(sql, (1))
        connection.commit()     # commitar o id deletado para DB
        
        # mostrando tabela depois do id Deletado
        cursor.execute(f'SELECT * FROM {TABLE_NAME} ')

        # for row in cursor.fetchall():
        #     print(row)        # retorna todas as linhas

    ######################
    print()

    # Editando com UPDATE, WHERE e placeholders no PyMySQL
    with connection.cursor() as cursor:
        cursor = cast(CURRENT_CURSOR, cursor)       # SSDictCursor

        sql = (
            f'UPDATE {TABLE_NAME} '
            'SET nome=%s, idade=%s WHERE id=%s'
        )
        cursor.execute(sql, ('Eleonor', 102, 4))
        # cursor.execute(f'SELECT * FROM {TABLE_NAME} ')

        # a) mostra no terminal resultado de pymysql.cursors.DictCursor
        # for row in cursor.fetchall():
        #     _id, name, age = row.items()  
        #     print(_id, name, age)  # mostra chave e valor dentro de tupla
           
        # b) mostra no terminal resultado de pymysql.cursors.DictCursor
        # for row in cursor.fetchall():
        #     print(row)      # mostra chave e valor (DictCursor) dentro de dicts

        # print()
        # # c) mostra resultado de SSDictCursor com fetchall_unbuffered
        # # não vai aparecer, se o cursor parou no 'for' anterior
        # print('For 1:')     
        # for row in cursor.fetchall_unbuffered():
        #     print(row)

        #     if row['id'] >= 5:  
        #         break      
        #     # no FOR 1 vai parar no id 5 e continuar no FOR 2

        # print()
        # print('For 2:')
        # # cursor.scroll(-1)  # 'scroll' mexe cursor / '-1' volta uma linha
        # # cursor.scroll(6, 'absolute')
        # for row in cursor.fetchall_unbuffered():   
        #     print(row)      # unbuffered > usado apenas com SSDictCursor / serve para consultas em dados grandes, sem salvar na memória / não consegue usar scroll, pois não volta linha por não salvar nada.

        # d) demais comandos (rowcount, rownumber, lastrowid)
        cursor.execute(
            f'SELECT id FROM {TABLE_NAME} ORDER BY id DESC LIMIT 1'
        )
        lastIdFromSelect = cursor.fetchone()  
        #DECRESCENTE LIMITADO a 1 {id: 8}

        resultFromSelect = cursor.execute(f'SELECT * FROM {TABLE_NAME}')

        data6 = cursor.fetchall()
        for row in data6:
            print(row)

        print('resultFromSelect', resultFromSelect)
        print('len(data6)', len(data6))
        print('rowcount', cursor.rowcount)
        print('lastrowid', cursor.lastrowid)    # último id inserido
        print('lastrowid na mão', lastIdFromSelect) #lastFrom...['id']

        cursor.scroll(0, 'absolute')
        print('rownumber', cursor.rownumber)
        # para saber em qual linha o cursor está

    connection.commit()

##################################################################

#cursor.close()
#connection.close()
# As conexões  e cursores são automaticamente fechadas quando são excluídas (normalmente quando saem do escopo) para que você não precise ligar [conn.close()], mas você pode fechar explicitamente a conexão, se desejar.

# Para carregar no DBEAVER, as vezes precisa apenas atualizar. E as vezes precisa desconectar, atualizar e conectar novamente.

# Usando 'executemany' precisa usar dados iteráveis, tupla de dict, ou tupla de tupla.

# fetchAll(): Retorna um array com todas as linhas da consulta, ideal para uma busca por nome ou por endereço.
# Também tem fetchmany e fetchone.

# SQL Injection >>> é um tipo de vulnerabilidade em que um invasor usa uma parte do código SQL (Structured Query Language) para manipular um banco de dados e obter acesso a informações potencialmente valiosas. É um tipo de ameaça de segurança que se aproveita de falhas em sistemas que trabalham com bases de dados realizando ataques com comandos SQL; onde o atacante consegue inserir uma instrução SQL. (Usar sempre placeholders e proibir no código a inserção de inteiro, parênteses, vírgula, ponto e vírgula, " ", #, >= <=, etc, usado em comandos).


# O método cursor.mogrify() retorna uma cadeia de caracteres de consulta uma vez que os parâmetros foram vinculados. Se você usou o método execute() ou qualquer coisa similar, a cadeia de caracteres retornada é a mesma que seria enviada para o banco de dados. A string resultante é sempre uma cadeia de bytes, que é mais rápida do que usar a função executemany(). O Psycopg, o driver do Python PostgreSQL, inclui um mecanismo muito útil para formatar SQL em Python, que é mogrify. / Após a ligação dos parâmetros, retorna uma string de consulta. A string retornada é a mesma que o SQL foi enviado para o banco de dados se você usou a função execute() ou qualquer coisa semelhante. Pode-se usar as mesmas entradas para mogrify() como você faria para execute(), e o resultado será como esperado.