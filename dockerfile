FROM 
COPY moveam-api moveam-api
COPY 
COPY 
COPY 
COPY requirements.txt /requirements.txt
RUN pip install -r requirements.txt
CMD uvicorn moveam-api.api:app --host 0.0.0.0 --reload --port $PORT