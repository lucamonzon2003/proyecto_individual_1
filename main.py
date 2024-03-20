from fastapi import FastAPI
from class_functions import func_x_endpoints

app = FastAPI()

@app.get("/{num}")
async def read_root(num : int):
    return {"message": num}

@app.get("/PTG/genero/{genero}")
def PlayTimeGenre (genero):
    try: 
        return func_x_endpoints.PlayTimeGenre(genero)
    except:
        return KeyError

@app.get("/UFG/genero/{genero}")
def UserForGenre (genero):
    return func_x_endpoints.UserForGenre(genero)

@app.get("/UR/anio/{anio}")
def UsersRecommend (anio : int):
    return func_x_endpoints.UsersRecommend(anio)

@app.get("/UNR/anio/{anio}")
def UsersNotRecommend (anio : int):
    return func_x_endpoints.UsersNotRecommend(anio)

@app.get("/SA/anio/{anio}")
def sentiment_analysis (anio : int):
    return func_x_endpoints.sentiment_analysis(anio)