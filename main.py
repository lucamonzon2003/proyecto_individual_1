from fastapi import FastAPI
from class_functions import func_x_endpoints

app = FastAPI()

@app.get("/{num}")
async def read_root(num : int):
    return {"message": num}

@app.get("/PTG/genero/{genero}")
def PlayTimeGenre (genero):
    class_functions = func_x_endpoints()
    return class_functions.PlayTimeGenre(genero)

@app.get("/UFG/genero/{genero}")
def UserForGenre (genero):
    class_functions = func_x_endpoints()
    return class_functions.UserForGenre(genero)

@app.get("/UR/anio/{anio}")
def UsersRecommend (anio : int):
    class_functions = func_x_endpoints()
    return class_functions.UsersRecommend(anio)

@app.get("/UNR/anio/{anio}")
def UsersNotRecommend (anio : int):
    class_functions = func_x_endpoints()
    return class_functions.UsersNotRecommend(anio)

@app.get("/SA/anio/{anio}")
def sentiment_analysis (anio : int):
    class_functions = func_x_endpoints()
    return class_functions.sentiment_analysis(anio)