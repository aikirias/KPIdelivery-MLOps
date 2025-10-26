# Notebooks

Colocá tus notebooks exploratorios en este directorio. El contenedor de JupyterLab monta `../notebooks` como carpeta de trabajo, por lo que todo lo que guardes acá quedará disponible fuera de Docker.

- `exploration.ipynb`: sugerido para exploración inicial de las fuentes.
- `churn_experiments.ipynb`: sugerido para pruebas de modelos en combinación con MLflow.

> Consejo: al conectarte desde el contenedor usa el token `mlops` (se puede ver/editar en `infrastructure/jupyterlab/Dockerfile`).
