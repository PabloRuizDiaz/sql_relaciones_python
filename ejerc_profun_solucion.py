'''
En este script se realizan las soluciones del ejercicio de profundizacion solicitadas en la clase.
'''

__author__ = "Pablo M. Ruiz Diaz"
__email__ = "rd.pablo@gmail.com"
__version__ = "1.0"

import csv
import sqlite3

from config import config


def insert_autor(group):
    conn = sqlite3.connect('libreria_sql.db')
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    try:
        c.executemany("""
                    INSERT INTO autores (author)
                    VALUES (?);
                    """, group)
    
    except sqlite3.Error as err:
        print(err)

    conn.commit()
    conn.close()


def insert_libro(group):
    conn = sqlite3.connect('libreria_sql.db')
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    try:
        c.executemany("""
                    INSERT INTO libros (title, pags, authors)
                    SELECT ?, ?, autores.id FROM autores WHERE autores.author = ? ;
                    """, group)
    
    except sqlite3.Error as err:
        print(err)

    conn.commit()
    conn.close()


def fetch(data_libro=0):
    conn = sqlite3.connect('libreria_sql.db')
    c = conn.cursor()

    if data_libro == 0:
        c.execute("""
                SELECT 
                    libros.id,
                    libros.title,
                    libros.pags,
                    autores.author
                FROM libros
                INNER JOIN autores ON libros.authors = autores.id;
                """)
    else:
        c.execute("""
                SELECT 
                    libros.id,
                    libros.title,
                    libros.pags,
                    autores.author
                FROM libros
                INNER JOIN autores ON libros.authors = autores.id
                WHERE libros.id = ?;
                """, (data_libro,))
    
    while True:
        row = c.fetchone()
    
        if row is None:
            break
    
        print(row)
    
    conn.close()


def search_author(book_title):
    conn = sqlite3.connect('libreria_sql.db')
    c = conn.cursor()

    c.execute("""
            SELECT 
                autores.author
            FROM libros
            INNER JOIN autores ON libros.authors = autores.id
            WHERE libros.title = ?;
            """, (book_title,))
    
    row = c.fetchone()
    
    conn.close()

    return row


if __name__ == '__main__':
    print("Bienvenidos a la resolucion del ejercicio de profundizacion!! :)")
    
    conn = sqlite3.connect('libreria_sql.db')
    c = conn.cursor()

    c.execute('DROP TABLE IF EXISTS autores;')
    c.execute('DROP TABLE IF EXISTS libros;')
        
    c.execute("""
            CREATE TABLE autores (
            [id] INTEGER PRIMARY KEY AUTOINCREMENT,
            [author] TEXT NOT NULL);
            """)
    
    c.execute("""
            CREATE TABLE libros (
            [id] INTEGER PRIMARY KEY AUTOINCREMENT,
            [title] TEXT NOT NULL,
            [pags] INTEGER,
            [authors] INTEGER NOT NULL REFERENCES autores(id));
            """)
    
    conn.commit()
    conn.close()
    
    chunksize = int(input('Ingrese valor de Chunksize: '))
        
    with open('libreria_autor.csv', 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        chunk = []
    
        for row in reader:
            copy_autores = row['autor']
            
            chunk.append([copy_autores])
            
            if len(chunk) == chunksize:
                insert_autor(chunk)
                chunk.clear()
        
        if chunk:
            insert_autor(chunk)

    with open('libreria_libro.csv', 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        chunk = []
    
        for row in reader:
            copy_libros = [row['titulo'], row['cantidad_paginas'], row['autor']]
            
            chunk.append(copy_libros)
            
            if len(chunk) == chunksize:
                insert_libro(chunk)
                chunk.clear()
        
        if chunk:
            insert_libro(chunk)
    
    # data_libro = int(input('Seleccione cual Libro desea ver informacion por ID: '))

    # fetch(data_libro)

    book_title = str(input('Ingrese nombre del libro: '))

    autor_libro = search_author(book_title)

    print(autor_libro)