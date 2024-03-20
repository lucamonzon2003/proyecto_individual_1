from fastapi import FastAPI
from class_functions import func_x_endpoints

app = FastAPI()

@app.get("/{num}")
async def read_root(num : int):
    return {"message": num}

@app.get("/PTG/genero/{genero}")
def PlayTimeGenre (genero):
    return PlayTimeGenre(genero)

@app.get("/UFG/genero/{genero}")
def UserForGenre (genero):

    return UserForGenre(genero)

@app.get("/UR/anio/{anio}")
def UsersRecommend (anio : int):
    return UsersRecommend(anio)

@app.get("/UNR/anio/{anio}")
def UsersNotRecommend (anio : int):
    return UsersNotRecommend(anio)

@app.get("/SA/anio/{anio}")
def sentiment_analysis (anio : int):
    return sentiment_analysis(anio)