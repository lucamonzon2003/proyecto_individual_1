import pandas as pd
import numpy as np

class func_x_endpoints:
    def __init__(self) -> None:
        self.user_items_df = pd.DataFrame(pd.read_parquet("./data_transformed/user_items.parquet"))
        self.reviews_df = pd.DataFrame(pd.read_parquet("./data_transformed/reviews.parquet"))
        self.games_df = pd.DataFrame(pd.read_parquet("./data_transformed/games.parquet"))

    def PlayTimeGenre(self, genero: str):
        # Filtrar juegos por género
        games_gnr = self.games_df.explode('genres')
        games_gnr = games_gnr[games_gnr['genres'] == genero]

        res = pd.merge(self.user_items_df, games_gnr, left_on='item_name', right_on='app_name', how='inner')
        res = res[res['release_date'] != 'Unknown']
        res['release_date'] = pd.to_datetime(res['release_date'])
        res_group = res[['release_date', 'playtime_forever']]
        res_group = res_group.groupby(pd.Grouper(key='release_date', freq='YE')).sum().reset_index()
        res_group = res_group.sort_values(by='playtime_forever', ascending=False)
        return {f"Año de lanzamiento con más horas jugadas para el Género {genero}": res_group['release_date'].iloc[0].year}
    def UserForGenre(self, genero: str):
        # Explorar los géneros para obtener un DataFrame con cada juego repetido por cada género al que pertenece
        games_gnr = self.games_df.explode('genres')
        # Filtrar los juegos por el género específico proporcionado como argumento
        games_gnr = games_gnr[games_gnr['genres'] == genero]
        
        # Fusionar los DataFrames de los elementos de usuario y juegos basados en el nombre del juego y la aplicación
        res = pd.merge(self.user_items_df, games_gnr, left_on='item_name', right_on='app_name', how='inner')
        
        # Filtrar las fechas de lanzamiento desconocidas y convertirlas al tipo de datos datetime
        res = res[res['release_date'] != 'Unknown']
        res['release_date'] = pd.to_datetime(res['release_date'])
        
        # Agrupar por steam_id y año de la fecha de lanzamiento, sumar las horas jugadas y restablecer el índice
        res_group = res[['user_id', 'steam_id', 'release_date', 'playtime_forever']]
        res_group = res_group.groupby(['steam_id', pd.Grouper(key='release_date', freq='YE')]).sum().reset_index()
        
        # Agrupar por steam_id, sumar las horas jugadas y restablecer el índice
        user_rank = res_group[['user_id', 'steam_id', 'playtime_forever']]
        user_rank = user_rank.groupby(['steam_id']).sum().reset_index()
        
        # Ordenar por horas jugadas en orden descendente y seleccionar la primera fila
        user_rank = user_rank.sort_values(by='playtime_forever', ascending=False)
        user_rank = user_rank.iloc[0]
        
        # Obtener el DataFrame del usuario con más horas jugadas
        user_group = res_group[res_group['steam_id'] == user_rank['steam_id']]
        # Extraer el año de la fecha de lanzamiento y convertirla a año
        user_group['release_date'] = user_group['release_date'].dt.year
        
        # Convertir el DataFrame a un diccionario con el año como clave y las horas jugadas como valor
        dicc_playtime = user_group.set_index('release_date')['playtime_forever'].to_dict()
        
        # Crear una lista de diccionarios con el año y las horas jugadas
        list_dicc = [{'Año': fecha, 'Horas': horas} for fecha, horas in dicc_playtime.items()]
        
        # Devolver un diccionario con el usuario con más horas jugadas para el género dado y las horas jugadas por año
        return {f'Usuario con más horas jugadas para Género X {genero}': user_rank['user_id'], 'Horas jugadas': list_dicc}
    def UsersRecommend(self, año: int):
        reviews = self.reviews_df[self.reviews_df['posted'].dt.year == año]
        cond = (reviews['recommend'] == True) & (reviews['feeling'] >= 1)
        reviews = reviews[cond]
        rank_games = reviews['item_id'].value_counts().iloc[0:3].to_frame()
        rank_games = pd.merge(rank_games, self.user_items_df, on='item_id', how='left')
        rank_games = rank_games['item_name'].unique()
        res = [{"Puesto " + str(i+1): valor} for i, valor in enumerate(rank_games)]
        return res
    def UsersNotRecommend(self, año: int):
        reviews = self.reviews_df[self.reviews_df['posted'].dt.year == año]
        cond = (reviews['recommend'] == False) & (reviews['feeling'] == 0)
        reviews = reviews[cond]
        rank_games = reviews['item_id'].value_counts().iloc[0:3].to_frame()
        rank_games = pd.merge(rank_games, self.user_items_df, on='item_id', how='left')
        rank_games = rank_games['item_name'].unique()
        res = [{"Puesto " + str(i+1): valor} for i, valor in enumerate(rank_games)]
        return res
    def sentiment_analysis(self, año: int):
        games = self.games_df[self.games_df['release_date'] != 'Unknown']
        games = games[pd.to_datetime(games['release_date']).dt.year == año]
        games_plus = pd.merge(games, self.user_items_df, left_on='app_name', right_on='item_name', how='left')
        games_plus = pd.merge(games_plus, self.reviews_df, on='item_id', how='left')
        count_dict = games_plus['feeling'].value_counts().to_dict()
        count_dict = {
            'Negative': count_dict.get(0),
            'Neutral': count_dict.get(1),
            'Positive': count_dict.get(2)
        }
        return count_dict
    
    