import pandas as pd
import numpy as np

class func_x_endpoints:
    @staticmethod

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
    
    def UserForGenre( genero: str):

        max_user_playtime = None
        user_playtime = None

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
        
            # Agrupar por usuario y año de la fecha de lanzamiento, sumar las horas jugadas y restablecer el índice
            current_user_playtime = games_gnr.groupby(['user_id', games_gnr['release_date'].dt.year])['playtime_forever'].sum().reset_index()

            if not games_gnr.empty:
                current_max_user_playtime = games_gnr.loc[games_gnr['playtime_forever'].idxmax()]
                if max_user_playtime is None or current_max_user_playtime['playtime_forever'] > max_user_playtime['playtime_forever']:
                        user_playtime = current_user_playtime
                        max_user_playtime = current_max_user_playtime
            del games_gnr
        
        # Obtener las horas jugadas por año del usuario con más horas jugadas
        user_year_playtime = user_playtime[user_playtime['user_id'] == max_user_playtime['user_id']]
        user_year_playtime = user_year_playtime.rename(columns={'release_date': 'Año', 'playtime_forever': 'Horas'})
        user_year_playtime = user_year_playtime[['Año', 'Horas']].to_dict(orient='records')

        # Devolver un diccionario con el usuario con más horas jugadas para el género dado y las horas jugadas por año
        return {'Usuario con más horas jugadas para Género X ' + genero: max_user_playtime['user_id'], 'Horas jugadas': user_year_playtime}
    
    def UsersRecommend( año: int):
        reviews = pd.DataFrame(pd.read_parquet("./data_transformed/reviews.parquet"))
        reviews = reviews[reviews['posted'].dt.year == año]
        reviews = reviews[(reviews['recommend'] == True) & (reviews['feeling'] >= 1)]
        # Contar las revisiones de cada juego y obtener los 3 juegos más populares
        top_games = reviews['item_id'].value_counts().head(3).index.tolist()

        del reviews

        rank_games = []
        
        # Inicializar un conjunto para almacenar juegos únicos
        unique_games = set()
        
        for i in range(10):
            file_path = f"./data_transformed/parts_user_items/part_user_items{i}.parquet"

            # Leer el archivo de datos de usuario
            df = pd.read_parquet(file_path)
        
            # Filtrar los juegos populares
            df = df[df['item_id'].isin(top_games)]
        
            # Agregar juegos únicos a la lista rank_games
            for game in df['item_name'].unique():
                if game not in unique_games:
                    rank_games.append(game)
                    unique_games.add(game)

            # Liberar la memoria eliminando el DataFrame actual
            del df

        res = [{"Puesto " + str(i+1): valor} for i, valor in enumerate(rank_games)]
        return res

    def UsersNotRecommend(año: int):
        # Leer los datos de las revisiones
        reviews = pd.read_parquet("./data_transformed/reviews.parquet")
    
        # Filtrar las revisiones por el año y el sentimiento positivo
        reviews = reviews[(reviews['posted'].dt.year == año) & (reviews['recommend'] == False) & (reviews['feeling'] < 1)]
    
        # Contar las revisiones de cada juego y obtener los 3 juegos menos populares
        least_recommended_games = reviews['item_id'].value_counts().tail(3).index.tolist()

        # Liberar la memoria eliminando el DataFrame de revisiones
        del reviews

        # Inicializar una lista para almacenar los juegos menos recomendados
        rank_games = []
    
        # Inicializar un conjunto para almacenar juegos únicos
        unique_games = set()

        # Iterar sobre los archivos de datos de usuario
        for i in range(10):
            file_path = f"./data_transformed/parts_user_items/part_user_items{i}.parquet"
        
            # Leer el archivo de datos de usuario
            df = pd.read_parquet(file_path)
        
            # Filtrar los juegos menos recomendados
            df = df[df['item_id'].isin(least_recommended_games)]
        
            # Agregar juegos únicos a la lista rank_games
            for game in df['item_name'].unique():
                if game not in unique_games:
                    rank_games.append(game)
                    unique_games.add(game)
        
            # Liberar la memoria eliminando el DataFrame actual
            del df

            # Si ya tenemos los 3 juegos menos recomendados, terminamos el bucle
            if len(rank_games) >= 3:
                break

        # Crear la lista de resultados con el puesto de cada juego
        res = [{"Puesto " + str(i+1): valor} for i, valor in enumerate(rank_games[:3])]
    
        return res

    def sentiment_analysis( año: int):
        # Leer los datos de los juegos y filtrar por el año
        games = pd.read_parquet("./data_transformed/games.parquet")
        games = games[games['release_date'] != 'Unknown']
        games = games[pd.to_datetime(games['release_date']).dt.year == año]
        games = games['app_name']

        # Leer los datos de los elementos de usuario y fusionar con los datos de juegos
        games_plus = pd.merge(games, pd.read_parquet("./data_transformed/user_items.parquet"), left_on='app_name', right_on='item_name', how='left')
        del games
        games_plus = games_plus['item_id']

        games_plus = pd.merge(games_plus, pd.read_parquet("./data_transformed/reviews.parquet"), on='item_id', how='left')
        
        # Contar el número de revisiones por sentimiento y convertir a un diccionario
        count_dict = games_plus['feeling'].value_counts().to_dict()

        del games_plus

        # Mapear los valores de sentimiento al diccionario
        count_dict = {
            'Negative': count_dict.get(0, 0),
            'Neutral': count_dict.get(1, 0),
            'Positive': count_dict.get(2, 0)
        }

        return count_dict
    
    # if __name__ == "__main__":
    #     PlayTimeGenre('Action')
    #     UserForGenre('Action')
    #     UsersRecommend(2000)
    #     UsersNotRecommend(2000)
    #     sentiment_analysis(2000)