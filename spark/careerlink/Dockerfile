# Use an official PySpark image as the base image
FROM python:3.8-slim

# Set the working directory
WORKDIR /app

# Copy the PySpark application code into the container
COPY . /app

# Set environment variables for Spark and Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/app/venv/Lib/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=/usr/local/bin/python


# For example, if you have a requirements.txt file, you can use:
RUN pip install -r /app/requirements.txt
RUN apt-get update && apt-get install -y openjdk-17-jdk

# Expose any ports your application needs (if applicable)
# EXPOSE 8080

# Specify the entry point for your application
# For example, if your PySpark script is named "main.py", use:
CMD ["python", "CareerlinkMain.py"]
