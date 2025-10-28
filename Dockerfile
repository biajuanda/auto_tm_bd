# Use the official AWS Lambda Python 3.11 base image
FROM public.ecr.aws/lambda/python:3.11

# Copy the requirements file first to leverage Docker layer caching
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install system dependencies for PostgreSQL and compilation
RUN yum install -y \
    gcc-c++ \
    make \
    postgresql-devel \
    && yum clean all

# Install Python dependencies directly into the task root for maximum compatibility
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt --target ${LAMBDA_TASK_ROOT}

# Copy the application code
COPY handler.py ${LAMBDA_TASK_ROOT}
COPY telemedida_service.py ${LAMBDA_TASK_ROOT}

# Set the command to your handler, which AWS Lambda will invoke
CMD [ "handler.lambda_handler" ]