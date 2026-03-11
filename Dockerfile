# 1. Use an official lightweight Python image
FROM python:3.11-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy your requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Install OS-level browser dependencies
# Running as root in Docker allows 'install-deps' to use apt-get successfully
RUN apt-get update && \
    python -m patchright install && \
    python -m patchright install-deps && \
    python -m camoufox fetch

# 5. Copy the rest of your application code
COPY . .

# 6. Start the FastAPI server using your existing programmatic entry point
CMD ["python", "main.py"]