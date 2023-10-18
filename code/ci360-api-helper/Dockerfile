# Use a PHP base image
FROM php:latest

LABEL company="SAS Institute"

# copy app into image
COPY . /var/www/html

# Set the working directory
WORKDIR /var/www/html

# Expose port 80
EXPOSE 80

# Start the PHP server
CMD ["php", "-S", "0.0.0.0:80"]
