FROM node:16.15.1-bullseye-slim AS assets
LABEL maintainer="Nick Janetakis <nick.janetakis@gmail.com>"

WORKDIR /app/assets

ARG UID=1000
ARG GID=1000

RUN apt-get update \
  && apt-get install -y build-essential \
  && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man \
  && apt-get clean \
  && groupmod -g "${GID}" node && usermod -u "${UID}" -g "${GID}" node \
  && mkdir -p /node_modules && chown node:node -R /node_modules /app

USER node

COPY --chown=node:node assets/package.json assets/*yarn* ./

RUN yarn install && yarn cache clean

ARG NODE_ENV="production"
ENV NODE_ENV="${NODE_ENV}" \
    PATH="${PATH}:/node_modules/.bin" \
    USER="node"

COPY --chown=node:node . ..

RUN if [ "${NODE_ENV}" != "development" ]; then \
  ../run yarn:build:js && ../run yarn:build:css; else mkdir -p /app/public; fi

CMD ["bash"]

###############################################################################

FROM python:3.10.5-slim-bullseye AS app
LABEL maintainer="Nick Janetakis <nick.janetakis@gmail.com>"

WORKDIR /app

RUN sed -i -e's/ main/ main contrib non-free archive stretch /g' /etc/apt/sources.list
RUN apt-get update && apt-get install -y build-essential curl libpq-dev python3-dev default-libmysqlclient-dev aria2 unrar p7zip curl python3 python3-pip ctorrent mariadb-client pv rclone gcc g++ make libzstd-dev wget git cmake ca-certificates curl gnupg sshpass p7zip-full p7zip-rar

# https://github.com/nodesource/distributions
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ENV NODE_MAJOR=20
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
RUN apt-get update && apt-get install nodejs -y
RUN npm install webtorrent-cli -g && webtorrent --version

RUN git clone --depth 1 https://github.com/martinellimarco/t2sz --branch v1.1.2
RUN mkdir t2sz/build
RUN cd t2sz/build && cmake .. -DCMAKE_BUILD_TYPE="Release" && make && make install

RUN rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man
RUN apt-get clean

COPY requirements*.txt ./
COPY bin/ ./bin

RUN chmod 0755 bin/* && bin/pip3-install

# Download models
RUN echo 'import ftlangdetect; ftlangdetect.detect("dummy")' | python3
RUN echo 'import sentence_transformers; sentence_transformers.SentenceTransformer("intfloat/multilingual-e5-small")' | python3

ARG FLASK_DEBUG="false"
ENV FLASK_DEBUG="${FLASK_DEBUG}" \
    FLASK_APP="allthethings.app" \
    FLASK_SKIP_DOTENV="true" \
    PYTHONUNBUFFERED="true" \
    PYTHONPATH="."

COPY --from=assets /app/public /public
COPY . .

# RUN if [ "${FLASK_DEBUG}" != "true" ]; then \
#   ln -s /public /app/public && flask digest compile && rm -rf /app/public; fi

ENTRYPOINT ["/app/bin/docker-entrypoint-web"]

EXPOSE 8000

CMD ["gunicorn", "-c", "python:config.gunicorn", "allthethings.app:create_app()"]
