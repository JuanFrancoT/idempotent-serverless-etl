FROM public.ecr.aws/lambda/python:3.11

# Copiar dependencias primero
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Instalar dependencias en el path correcto
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copiar código
COPY app.py ${LAMBDA_TASK_ROOT}

# Comando de ejecución
CMD ["app.lambda_handler"]