FROM python:3.10-slim

WORKDIR /app
# Copy the main Python script
COPY home_2.py /app/
COPY config.py /app/
# Copy the pages directory and its contents
COPY pages_2 /app/pages_2

COPY requirements.txt /app/
COPY .streamlit /app/.streamlit

RUN pip install -r ./requirements.txt
EXPOSE 8501
CMD ["streamlit", "run", "home_2.py"]

