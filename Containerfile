ARG DOCKER_MIRROR=docker.io
FROM $DOCKER_MIRROR/library/python:3.10-slim
ARG USERNAME=pydeid
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG BUILD_ID=0
ENV FLASK_APP=py_de_id.app:create_app
ENV HOME=/app
ENV PATH=/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/app/.local/bin
ENV BUILD_ID=$BUILD_ID
RUN apt update -y && apt install -y procps

WORKDIR /app

RUN addgroup --gid $USER_GID $USERNAME 
RUN adduser --uid $USER_UID $USERNAME --system --ingroup $USERNAME

RUN chown $USERNAME: /app

USER $USERNAME

COPY startup readme.md pyproject.toml /app/

ADD tests/* /app/tests/
ADD py_de_id/*   /app/py_de_id/
ADD assets/* /app/assets/
RUN pip install --upgrade pip trustme pytest coverage .
RUN python -m trustme
#RUN coverage run -m pytest

#RUN coverage html --omit="*/test*"

#ENTRYPOINT ["flask", "run", "--host", "0.0.0.0", "--port", "5000"]
ENTRYPOINT ["/app/startup"]
EXPOSE 5000