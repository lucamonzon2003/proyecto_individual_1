from fastapi import FastAPI
from class_functions import func_x_endpoints

app = FastAPI()
class_functions = func_x_endpoints()

@app.get("/")
async def read_root():
    return {"message": "Hello, World"}

@app.get("/PTG/genero/{genero}")
def PlayTimeGenre (genero):
    return class_functions.PlayTimeGenre(genero)

@app.get("/UFG/genero/{genero}")
def UserForGenre (genero):
    return class_functions.UserForGenre(genero)

@app.get("/UR/año/{año}")
def UsersRecommend (año):
    return class_functions.UsersRecommend(año)

@app.get("/UNR/año/{año}")
def UsersNotRecommend (año):
    return class_functions.UsersNotRecommend(año)

@app.get("/SA/año/{año}")
def sentiment_analysis (año):
    return class_functions.sentiment_analysis(año)