# Use Python 3.11 (tensorflow support)
FROM python:3.11-slim

# Start from Jupyter's minimal notebook environment
FROM jupyter/minimal-notebook:latest

# Set the working directory inside the container
WORKDIR /home/jovyan/work

# Copy requirements.txt if you have one and install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt

# Expose Jupyter's default port
EXPOSE 8888

# Start JupyterLab when the container runs
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
