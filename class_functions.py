import pandas as pd
import numpy as np
from memory_profiler import profile

class func_x_endpoints:
    @staticmethod
    @profile
    def PlayTimeGenre(genero: str):
        max_playtime_year = None
        # Leer los archivos de juegos en lotes
        games = pd.DataFrame(pd.read_parquet(f"./data_transformed/genres_games/games_{genero.lower().replace(' ', '_')}.parquet"))
        file_paths = [
            './data_transformed/parts_user_items/part_user_items0.parquet',
            './data_transformed/parts_user_items/part_user_items1.parquet',
            './data_transformed/parts_user_items/part_user_items2.parquet',
            './data_transformed/parts_user_items/part_user_items3.parquet',
            './data_transformed/parts_user_items/part_user_items4.parquet',
            './data_transformed/parts_user_items/part_user_items5.parquet',
            './data_transformed/parts_user_items/part_user_items6.parquet',
            './data_transformed/parts_user_items/part_user_items7.parquet',
            './data_transformed/parts_user_items/part_user_items8.parquet',
            './data_transformed/parts_user_items/part_user_items9.parquet']
        for file_path in file_paths:
                games_gnr = pd.merge(pd.DataFrame(pd.read_parquet(file_path)), games, left_on='item_name', right_on='app_name', how='inner')
                
                # Filtrar las fechas de lanzamiento desconocidas y convertirlas al tipo de datos datetime
                games_gnr = games_gnr[games_gnr['release_date'] != 'Unknown']
                games_gnr['release_date'] = pd.to_datetime(games_gnr['release_date'])
                
                

                # Obtener el año de lanzamiento con más horas jugadas en el lote actual
                if not games_gnr.empty:
                    current_max_year = games_gnr.groupby(games_gnr['release_date'].dt.year)['playtime_forever'].sum().idxmax()
                    if max_playtime_year is None or games_gnr['playtime_forever'].sum() > max_playtime_year.sum():
                        max_playtime_year = current_max_year
                del games_gnr
            
        return {f"Año de lanzamiento con más horas jugadas para el Género {genero}": max_playtime_year}
    
    @profile
    def UserForGenre( genero: str):
        # Explorar los géneros para obtener un DataFrame con cada juego repetido por cada género al que pertenece
        games_gnr = pd.DataFrame(pd.read_parquet("./data_transformed/games.parquet")).explode('genres')
        # Filtrar los juegos por el género específico proporcionado como argumento
        games_gnr = games_gnr[games_gnr['genres'] == genero]
        
        # Fusionar los DataFrames de los elementos de usuario y juegos basados en el nombre del juego y la aplicación
        games_gnr = pd.merge(pd.DataFrame(pd.read_parquet("./data_transformed/user_items.parquet")), games_gnr, left_on='item_name', right_on='app_name', how='inner')
        
        # Filtrar las fechas de lanzamiento desconocidas y convertirlas al tipo de datos datetime
        games_gnr = games_gnr[games_gnr['release_date'] != 'Unknown']
        games_gnr['release_date'] = pd.to_datetime(games_gnr['release_date'])
        
        # Agrupar por año de la fecha de lanzamiento, sumar las horas jugadas y restablecer el índice
        games_gnr = games_gnr[['user_id', 'release_date', 'playtime_forever']]
        games_gnr = games_gnr.groupby(['user_id', pd.Grouper(key='release_date', freq='YE')]).sum().reset_index()
        
        # Agrupar, sumar las horas jugadas y restablecer el índice
        user_rank = games_gnr[['user_id', 'playtime_forever']]
        user_rank = user_rank.groupby('user_id').sum().reset_index()
        
        # Ordenar por horas jugadas en orden descendente y seleccionar la primera fila
        user_rank = user_rank.sort_values(by='playtime_forever', ascending=False)
        user_rank = user_rank.iloc[0]
        
        # Obtener el DataFrame del usuario con más horas jugadas
        user_year = games_gnr[games_gnr['user_id'] == user_rank['user_id']]
        # Extraer el año de la fecha de lanzamiento y convertirla a año
        user_year['release_date'] = user_year['release_date'].dt.year
        
        # Convertir el DataFrame con el año como clave y las horas jugadas como valor
        dicc_playtime = user_year.set_index('release_date')['playtime_forever'].to_dict()
        
        # # Crear una lista de diccionarios con el año y las horas jugadas
        list_dicc = [{'Año': fecha, 'Horas': horas} for fecha, horas in dicc_playtime.items()]
        # Devolver un diccionario con el usuario con más horas jugadas para el género dado y las horas jugadas por año
        return  {f'Usuario con más horas jugadas para Género X {genero}': user_rank['user_id'], 'Horas jugadas': list_dicc}
    @profile
    def UsersRecommend( año: int):
        reviews = pd.DataFrame(pd.read_parquet("./data_transformed/reviews.parquet"))
        reviews = reviews[reviews['posted'].dt.year == año]
        cond = (reviews['recommend'] == True) & (reviews['feeling'] >= 1)
        reviews = reviews[cond]
        rank_games = reviews['item_id'].value_counts().iloc[0:3].to_frame()
        rank_games = pd.merge(rank_games, pd.DataFrame(pd.read_parquet("./data_transformed/user_items.parquet")), on='item_id', how='left')
        rank_games = rank_games['item_name'].unique()
        res = [{"Puesto " + str(i+1): valor} for i, valor in enumerate(rank_games)]
        return res
    @profile
    def UsersNotRecommend( año: int):
        reviews = pd.DataFrame(pd.read_parquet("./data_transformed/reviews.parquet"))
        reviews = reviews[reviews['posted'].dt.year == año]
        cond = (reviews['recommend'] == False) & (reviews['feeling'] == 0)
        reviews = reviews[cond]
        rank_games = reviews['item_id'].value_counts().iloc[0:3].to_frame()
        rank_games = pd.merge(rank_games, pd.DataFrame(pd.read_parquet("./data_transformed/user_items.parquet")), on='item_id', how='left')
        rank_games = rank_games['item_name'].unique()
        res = [{"Puesto " + str(i+1): valor} for i, valor in enumerate(rank_games)]
        return res
    @profile
    def sentiment_analysis( año: int):
        games = pd.DataFrame(pd.read_parquet("./data_transformed/games.parquet"))
        games = games[games['release_date'] != 'Unknown']
        games = games[pd.to_datetime(games['release_date']).dt.year == año]
        games_plus = pd.merge(games, pd.DataFrame(pd.read_parquet("./data_transformed/user_items.parquet")), left_on='app_name', right_on='item_name', how='left')
        games_plus = pd.merge(games_plus, pd.DataFrame(pd.read_parquet("./data_transformed/reviews.parquet")), on='item_id', how='left')
        count_dict = games_plus['feeling'].value_counts().to_dict()
        count_dict = {
            'Negative': count_dict.get(0),
            'Neutral': count_dict.get(1),
            'Positive': count_dict.get(2)
        }
        return count_dict
    
    if __name__ == "__main__":
        PlayTimeGenre('Action')
        # UserForGenre('Action')
        # UsersRecommend(2000)
        # UsersNotRecommend(2000)
        # sentiment_analysis(2000)