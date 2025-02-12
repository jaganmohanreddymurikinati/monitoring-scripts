# Step 1: Use the official Python image as a base
FROM python:3.8-slim

# Step 2: Set the working directory inside the container
WORKDIR /app

# Step 3: Copy the local code to the container
COPY . /app

# Step 4: Install required Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Step 5: Expose any necessary ports (if needed)
# EXPOSE 8080

# Step 6: Command to run the application
CMD ["python", "monitoringscripts.py"]
