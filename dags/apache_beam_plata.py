import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import re
import pyarrow

class TransformacionHTML(beam.DoFn): #Tengo que pasarle el beam.DoFn para marcar que esta clase va a ejecutar una tarea de apache beam

    def process(self, archivo): #Por defecto la primera funcion que ejecuta apache beam
        '''
        1. Vamos a procesar el HTML
        2. Lo vamos a limpiar
        3. Lo vamos a estandarizar
        4. Se va a subir a google cloud storage
        '''
        nombre = archivo.metadata.path #path de google cloud storage
        df_lista = pd.read_html(archivo.read().decode('utf-8'), flavor="bs4") #Retorna una lista de dataframes
        df = df_lista[0]
        print("Procesando temporada:", nombre)
        df_nuevo = self.limpieza(df, nombre) #99-2000 1998-99
        temporada = re.search(r'(\d{4}-\d{2}|\d{2}-\d{4})', nombre).group() #tipo string con la temporada
        if not df_nuevo.empty and len(df_nuevo.columns) == 9:
            df_nuevo['temporada'] = temporada
            for columna in df_nuevo.columns:
                if columna != 'equipo' and columna != 'temporada':
                    df_nuevo[columna] = df_nuevo[columna].astype(int)
                else:
                    df_nuevo[columna] = df_nuevo[columna].astype(str)
                
            for _, fila in df_nuevo.iterrows():
                yield fila.to_dict()
        else:
            print("No se encontraron todas las columnas", nombre)
        return

    def limpieza(self, df, nombre):
        df_clasificaciones = df.iloc[:, 0:11]
        df_clasificaciones = self.obteniendo_columnas(df)
        df_clasificaciones = self.estandarizacion(df_clasificaciones)
        return df_clasificaciones

    def estandarizacion(self, df):
        columnas_estandarizar = {
            'Equipo' : 'equipo',
            'Pos' : 'posicion',
            'Pos.' : 'posicion',
            'Pts' : 'puntos',
            'Pts.' : 'puntos',
            'G' : 'partidos_ganados',
            'PG' : 'partidos_ganados',
            'E' : 'partidos_empatados',
            'PE' : 'partidos_empatados',
            'P' : 'partidos_perdidos',
            'PP' : 'partidos_perdidos',
            'GF' : 'goles_favor',
            'GC' : 'goles_contra',
            'PJ' : 'partidos_jugados'
        }
        mapeo_final = {
            viejo: nuevo for viejo, nuevo in columnas_estandarizar.items()
            if viejo in df.columns
        }

        df = df.rename(columns=mapeo_final) # Ponerle el nombre nuevo a las columnas viejas
        return df

    def obteniendo_columnas(self, df):
        columnas_utilizar = {
            'Equipo' : [],
            'Pos' : ['Pos.'],
            'Pts' : ['Pts.'],
            'G' : ['PG'],
            'E' : ['PE'],
            'P' : ['PP'],
            'GF' : [],
            'GC' : [],
            'PJ' : []
        }

        columnas_obtener = set(columnas_utilizar.keys())
        for valor in columnas_utilizar.values():
            columnas_obtener.update(valor)

        psalida_df = df[[col for col in df.columns if col in columnas_obtener]] #Iterando por las columnas del df y buscando si existe en mi set (columnas_obtener) -> retorne la columna
        return psalida_df
            

entrada = 'gs://melodic-subject-467218-g1-proyecto-datos/bronce/clasificaciones/*.html'
salida = 'gs://melodic-subject-467218-g1-proyecto-datos/plata/clasificacion/'

with beam.Pipeline(options=PipelineOptions()) as p:
    (p
    | 'Obtener archivos' >> fileio.MatchFiles(entrada) #Estoy obteniendo archivos -> Retornando una coleccion de archivos
    | 'Leer Archivos' >> fileio.ReadMatches() #Esto leyendolos -> string con el valor de cada archivo
    | 'Procesar Archivos' >> beam.ParDo(TransformacionHTML()) #Va a ir a la clase a ejecutar el codigo que yo defina dentro
    | 'Subir al bucket' >> beam.io.WriteToParquet(salida, pyarrow.schema(
        [
            ('posicion', pyarrow.int32()),
            ('equipo', pyarrow.string()),
            ('puntos', pyarrow.int32()),
            ('partidos_jugados', pyarrow.int32()),
            ('partidos_ganados', pyarrow.int32()),
            ('partidos_empatados', pyarrow.int32()),
            ('partidos_perdidos', pyarrow.int32()),
            ('goles_favor', pyarrow.int32()),
            ('goles_contra', pyarrow.int32()),
            ('temporada', pyarrow.string())
        ]
    )))