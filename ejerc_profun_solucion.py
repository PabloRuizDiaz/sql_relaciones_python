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
                    INSERT INTO libros (title, pags, author)
                    SELECT ?, ?, autores.id FROM libros, autores WHERE autores.author = ? ;
                    """, group)
    
    except sqlite3.Error as err:
        print(err)

    conn.commit()
    conn.close()


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
            [author] INTEGER NOT NULL REFERENCES autor(id));
            """)
    
    chunksize = int(input('Ingrese valor de Chunksize: '))
        
    with open('libreria_autor.csv', 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        chunk = []
    
        for row in reader:
            copy_autores = row['autor']
            
            chunk.append(copy_autores)
            
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
    
    conn.commit()
    conn.close()
    