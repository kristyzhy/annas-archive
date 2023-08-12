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

RUN sed -i -e's/ main/ main contrib non-free/g' /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y build-essential curl libpq-dev python3-dev default-libmysqlclient-dev aria2 unrar curl python3 python3-pip ctorrent mariadb-client pv rclone gcc g++ make
# https://github.com/nodesource/distributions#using-debian-as-root
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && apt-get install -y nodejs
RUN npm install webtorrent-cli -g && webtorrent --version

RUN rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man
RUN apt-get clean

COPY requirements*.txt ./
COPY bin/ ./bin

RUN chmod 0755 bin/* && bin/pip3-install

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
