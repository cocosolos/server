# syntax=docker/dockerfile:1-labs

########
# Base #
########
FROM ubuntu:latest AS base

ARG DEBIAN_FRONTEND=noninteractive

RUN <<EOF
rm -f /etc/apt/apt.conf.d/docker-clean
echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
EOF

# Install runtime dependencies.
RUN apt-get update && apt-get install --assume-yes --no-install-recommends --quiet \
    binutils \
    ca-certificates \
    git \
    libluajit-5.1-2 \
    libssl3t64 \
    libzmq5 \
    mariadb-client \
    python3 \
    tini \
    tzdata \
    zlib1g

RUN git config --system --add safe.directory /server
ENV PATH=/xiadmin/.local/bin:$PATH

ARG UNAME=xiadmin
ARG UGROUP=xiadmin
ARG UID=1000
ARG GID=1000

RUN <<EOF
userdel --remove ubuntu
groupadd --gid $GID $UNAME
useradd  --uid $UID $UNAME --gid $UGROUP --home-dir /xiadmin --create-home
EOF

###########
# Staging #
###########
FROM base AS staging

# Install build dependencies.
RUN --mount=type=cache,target=/var/cache/apt,id=var-cache-apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,id=var-lib-apt,sharing=locked \
    apt-get update && apt-get install --assume-yes --no-install-recommends --quiet \
    binutils-dev \
    build-essential \
    ccache \
    clang-18 \
    cmake \
    g++-14 \
    libluajit-5.1-dev \
    libmariadb-dev-compat \
    libssl-dev \
    libzmq3-dev \
    make \
    ninja-build \
    python3-dev \
    python3-pip \
    zlib1g-dev

USER $UNAME
WORKDIR /server

# Install Python dependencies.
RUN --mount=type=bind,source=./tools/requirements.txt,target=/tmp/requirements.txt \
    --mount=type=cache,target=/xiadmin/.cache/pip,id=cache-pip-ubuntu,uid=$UID,gid=$GID \
    pip3 install --break-system-packages --user --ignore-installed --requirement /tmp/requirements.txt

# Exclude changes to git metadata, scripts, and sql not needed for build.
# Excluded here instead of dockerignore so they can be bind mounted during build.
# Saves from copying everything whenever scripts/sql change.
# https://docs.docker.com/reference/dockerfile/#copy---exclude (docker/dockerfile:1.7-labs)
COPY --chown=$UNAME:$UGROUP --exclude=.git --exclude=losmeshes/** --exclude=navmeshes/** --exclude=scripts --exclude=sql . /server

#########
# Build #
#########
FROM staging AS build

ARG COMPILER=gcc
ARG CMAKE_BUILD_TYPE=Release

RUN <<EOF
if [ $COMPILER = "clang" ]; then
    export CC=/usr/bin/clang-18
    export CXX=/usr/bin/clang++-18
else
    export CC=/usr/bin/gcc-14
    export CXX=/usr/bin/g++-14
fi
EOF

ENV CCACHE_DIR=/xiadmin/.ccache
RUN --mount=type=cache,target=/xiadmin/build,id=build-ubuntu-$COMPILER,uid=$UID,gid=$GID \
    --mount=type=cache,target=/xiadmin/.ccache,id=ccache-ubuntu-$COMPILER,uid=$UID,gid=$GID \
    --mount=type=bind,source=./.git,target=/server/.git \
    --mount=type=bind,source=./scripts,target=/server/scripts \
    --mount=type=bind,source=./sql,target=/server/sql \
    # --- CACHE ---
    cp -p /xiadmin/build/version.cpp /server/src/common/; \
    cp -p /xiadmin/build/xi_* /server/; \
    # --- End ---
    cmake -G Ninja -S /server -B /xiadmin/build && \
    cmake --build /xiadmin/build -j$(nproc) && \
    # --- CACHE ---
    cp -p /server/xi_* /xiadmin/build/ && \
    cp -p /server/src/common/version.cpp /xiadmin/build/
    # --- End ---

###########
# Service #
###########
FROM base AS service

USER $UNAME
WORKDIR /server

ARG REPO_URL
ARG BRANCH

COPY --chown=$UNAME:$UGROUP --from=staging /xiadmin/.local /xiadmin/.local

COPY --chown=$UNAME:$UGROUP ./res/compress.dat res/decompress.dat /server/res/
COPY --chown=$UNAME:$UGROUP ./scripts /server/scripts
COPY --chown=$UNAME:$UGROUP ./sql /server/sql
COPY --chown=$UNAME:$UGROUP ./tools /server/tools
COPY --chown=$UNAME:$UGROUP ./modules /server/modules
COPY --chown=$UNAME:$UGROUP ./settings /server/settings

COPY --chown=$UNAME:$UGROUP --from=build /server/xi_* /server/

RUN <<EOF
git clone --bare --filter=tree:0 --branch $BRANCH $REPO_URL /server/.git
cd /server/.git && git config --local --bool core.bare false
EOF

COPY --chmod=0755 entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/bin/bash"]