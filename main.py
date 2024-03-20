from fastapi import FastAPI
from fastapi import HTTPException
from class_functions import func_x_endpoints

app = FastAPI()

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