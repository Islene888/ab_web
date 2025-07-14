# Stage 1: Build the React application
FROM node:20 AS build

WORKDIR /app

# Copy package files and install dependencies first
COPY package*.json ./
RUN npm install

# Copy the rest of the application source code
COPY . .

# Build the application
RUN npm run build

# Stage 2: Serve the application with NGINX
FROM nginx:1.25-alpine

# Copy the build output from the build stage
COPY --from=build /app/build /usr/share/nginx/html

# Expose port 80 for the NGINX server
EXPOSE 80

# Command to start NGINX in the foreground
CMD ["python", "-u", "-m", "backend.main"]
