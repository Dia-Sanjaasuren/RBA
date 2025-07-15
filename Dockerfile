FROM python:3.10-slim

WORKDIR /app
# Copy all files in the repository to the container
COPY . /app/

RUN pip install -r ./requirements.txt
EXPOSE 8501
CMD ["streamlit", "run", "home_2.py", "--server.address=0.0.0.0"]

