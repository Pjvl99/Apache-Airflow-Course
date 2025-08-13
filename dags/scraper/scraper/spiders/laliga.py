import scrapy
import pandas as pd
import os

class LaligaSpider(scrapy.Spider):
    name = "laliga" #El nombre de mi arana
    start_urls = ["https://es.wikipedia.org/wiki/Primera_Divisi%C3%B3n_de_Espa%C3%B1a"] #La primera url a la que va a ir

    def parse(self, response): #La primera funcion que se va a ejecutar
        '''
        Pasos:
          1. Voy a ir a la url principal de la liga y voy a tomar mi tabla historica de temporadas para tener referencias de si existe o no la temporada.
        '''
        
        if response.status == 200: #El sitio fue recibido exitosamente
            tabla_temporadas = response.css('table[class^="sortable col2izq col3izq"]') #Obtengo mi tabla de temporadas (todo el html)
            hrefs = tabla_temporadas.css('tbody > tr > td:first-child > a').xpath("@href").getall() #Obtengo las url de mi tabla de temporadas
            for href in hrefs:
                print("Procesando temporada:", href)
                yield response.follow(href, callback=self.procesar_datos) #El callback es la funcion de python que va a ejecutar la logica de la siguiente url
        else:
            raise Exception("Error en la conexion: ", response.url, response.status)
        
    def almacenado_datos(self, tabla, folder, temporada):
        '''
        Almacenar el html en archivos por categoria y temporada para que se vea ordenado (localmente)
        '''
        path = f'/usr/local/airflow/datos/{folder}'
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f'{path}/{temporada}.html', 'w') as f:
            f.write(tabla.get())
            f.close()
        return True

    def procesar_datos(self, response):
        '''
        Pasos:
          1. Obtener la informacion por temporada (tabla de liga, goleadores, etc)
        '''
        if response.status == 200:
            tabla_goleadores_existe = False
            tabla_resultados_existe = False
            tabla_posiciones_historicas_existe = False
            tabla_clasificacion_existe = False
            temporada = response.url[-7:]
            tablas = response.css('table')
            print("Extrayendo tablas:", temporada)
            for tabla in tablas: #Todas las tablas de cada temporada
                tabla_goleadores = tabla.css("tbody > tr > th:nth-child(4)").get()
                if not tabla_goleadores_existe and tabla_goleadores and "goles" in tabla_goleadores.lower():
                    tabla_goleadores_existe = True
                    self.almacenado_datos(tabla, "goleadores", temporada)
                
                referencia_resultados = tabla.css("tbody > tr > th:first-child::text").get()
                if referencia_resultados and "local" in referencia_resultados.lower() and "visitante" in referencia_resultados.lower():
                    tabla_resultados_existe = True
                    self.almacenado_datos(tabla, "resultados", temporada)
                
                referencia_posiciones_historicas = tabla.xpath("@class").get()
                if referencia_posiciones_historicas and "wikitable" in referencia_posiciones_historicas:
                    df = pd.read_html(tabla.get())[0]
                    if len(df.columns) >= 10 and not tabla_posiciones_historicas_existe:
                        tabla_posiciones_historicas_existe = True
                        self.almacenado_datos(tabla, "posiciones_historicas", temporada)
                
                referencia_tabla_clasificaciones = tabla.css("tbody > tr > th:nth-child(2)").get()
                if referencia_tabla_clasificaciones and "equipo" in referencia_tabla_clasificaciones.lower():
                    df = pd.read_html(tabla.get())[0]
                    if len(df) >= 10:
                        self.almacenado_datos(tabla, "clasificaciones", temporada)
                
        else:
            raise Exception("Error en la conexion: ", response.url, response.status)
