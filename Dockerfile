# Dockerfile References: https://docs.docker.com/engine/reference/builder/

# Start from python:3.8-alpine base image
FROM python:3.9-alpine

# The latest alpine images don't have some tools like (`git` and `bash`).
# Adding git, bash and openssh to the image
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh

# Make dir app
RUN mkdir /app
WORKDIR /app
COPY ./proxy_rev_py/requirements.txt requirements.txt

RUN pip install -r requirements.txt

# Copy the source from the current directory to the Working Directory inside the container
COPY ./proxy_rev_py/ .


#COPY ./proxy_rev_py/ .

# Expose port 8080 to the outside world
EXPOSE 8888

# Run the executable
CMD ["python", "proxy.py", "--port", "8888", "--hostname", "servicebus2.caixa.gov.br"]