from fastapi import FastAPI
from fastapi import HTTPException
from class_functions import func_x_endpoints
import pandas as pd

app = FastAPI()

df_recomendacion_juego = pd.read_csv('df_recomendacion_juego.csv', header=0)

@app.get("/{num}")
async def read_root(num : int):
    return {"message": num}

@app.get("/PTG/genero/{genero}")
def PlayTimeGenre (genero):
    try:
        result = func_x_endpoints.PlayTimeGenre(genero)
        # Convertir el resultado a JSON
        return result
    except Exception as e:
        # Manejar cualquier excepción que pueda ocurrir durante la serialización
        raise HTTPException(status_code=500, detail="Error al procesar la solicitud")

@app.get("/UFG/genero/{genero}")
def UserForGenre (genero):
    try:
        result = func_x_endpoints.UserForGenre(genero)
        # Convertir el resultado a JSON
        return result
    except Exception as e:
        # Manejar cualquier excepción que pueda ocurrir durante la serialización
        raise HTTPException(status_code=500, detail="Error al procesar la solicitud")

@app.get("/UR/anio/{anio}")
def UsersRecommend (anio : int):
    try:
        result = func_x_endpoints.UsersRecommend(anio)
        # Convertir el resultado a JSON
        return result
    except Exception as e:
        # Manejar cualquier excepción que pueda ocurrir durante la serialización
        raise HTTPException(status_code=500, detail="Error al procesar la solicitud")

@app.get("/UNR/anio/{anio}")
def UsersNotRecommend (anio : int):
    try:
        result = func_x_endpoints.UsersNotRecommend(anio)
        # Convertir el resultado a JSON
        return result
    except Exception as e:
        # Manejar cualquier excepción que pueda ocurrir durante la serialización
        raise HTTPException(status_code=500, detail="Error al procesar la solicitud")
    
@app.get("/SA/anio/{anio}")
def sentiment_analysis (anio : int):
    try:
        result = func_x_endpoints.sentiment_analysis(anio)
        # Convertir el resultado a JSON
        return result
    except Exception as e:
        # Manejar cualquier excepción que pueda ocurrir durante la serialización
        raise HTTPException(status_code=500, detail="Error al procesar la solicitud")

@app.get()
def game_recommended(id: int):
    df_id_juego = df_recomendacion_juego[df_recomendacion_juego['id_juego'] == id_juego]
    if df_id_juego.empty:
        return {"No se encontraron datos para el id {}".format(id_juego)}
    top = df_id_juego.head(5)
    nombre_juego = df_id_juego['nombre_juego'].iloc[0]
    juegos_recomendados = {'Juegos recomendados para el juego {}:'.format(nombre_juego): set(top['juego'])}
    return juegos_recomendados