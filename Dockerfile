FROM python:3.8

ARG XL_IDP_PATH_DOCKER

ENV XL_IDP_PATH_DOCKER=$XL_IDP_PATH_DOCKER

RUN chmod -R 777 $XL_IDP_PATH_DOCKER

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN python3 -m spacy download en_core_web_sm
