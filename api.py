import uvicorn
from src.main import app



    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, reload=True)
