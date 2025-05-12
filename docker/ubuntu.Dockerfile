# syntax=docker/dockerfile:1-labs

########
# Base #
########
FROM ubuntu:latest AS base

ARG DEBIAN_FRONTEND=noninteractive
RUN rm -f /etc/apt/apt.conf.d/docker-clean && \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' \
    > /etc/apt/apt.conf.d/keep-cache

# Install runtime dependencies.
RUN apt-get update && apt-get install --assume-yes --no-install-recommends --quiet \
    bash \
    binutils \
    ca-certificates \
    git \
    libzmq5 \
    luajit \
    mariadb-client \
    openssl \
    python3 \
    screen \
    tini \
    tzdata \
    zlib1g

# Setup runtime user.
ARG UNAME=xiadmin
ARG UGROUP=xiadmin
ARG UID=1000
ARG GID=1000
RUN userdel --remove ubuntu && \
    groupadd --gid $GID $UNAME && \
    useradd  --uid $UID $UNAME --gid $UGROUP --home-dir /xiadmin --create-home

WORKDIR /server
RUN chown $UNAME:$UGROUP /server
RUN git config --system --add safe.directory /server

ENV VIRTUAL_ENV=/xiadmin/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

SHELL ["/bin/bash", "-c"]

###########
# Staging #
###########
FROM base AS staging

# Install build dependencies.
RUN --mount=type=cache,target=/var/cache/apt,id=cache-apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,id=lib-apt,sharing=locked \
    apt-get update && apt-get install --assume-yes --no-install-recommends --quiet \
    binutils-dev \
    build-essential \
    ccache \
    cmake \
    g++-14 \
    libluajit-5.1-dev \
    libmariadb-dev-compat \
    libssl-dev \
    libzmq3-dev \
    make \
    ninja-build \
    python3-dev \
    python3-venv \
    zlib1g-dev

# Install Python dependencies.
RUN --mount=type=bind,source=tools/requirements.txt,target=/tmp/requirements.txt \
    --mount=type=cache,target=/root/.cache/pip,id=cache-pip-ubuntu \
    python3 -m venv $VIRTUAL_ENV && \
    pip3 install --requirement /tmp/requirements.txt

############
# devtools #
############
FROM staging AS devtools

# Install misc dev/ci tools on top of build tools.
RUN apt-get update && apt-get install --assume-yes --no-install-recommends --quiet \
    clang-format-18 \
    cppcheck \
    gcc \
    luarocks \
    && rm -rf /var/lib/apt/lists/*
RUN luarocks install luacheck --tree /root/.luarocks

COPY --chmod=0755 entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/bin/bash"]

#########
# Build #
#########
FROM staging AS build

ARG COMPILER=gcc14
RUN <<EOF
if [[ $COMPILER == clang* ]]; then
    apt-get update && apt-get install --assume-yes --no-install-recommends --quiet \
    clang-18 \
    lld-18 \
    libc++-18-dev \
    libc++abi-18-dev \
    libclang-rt-18-dev \
    libunwind-18-dev \
    && rm -rf /var/lib/apt/lists/*
fi
EOF

# Exclude changes to git metadata, scripts, and sql not needed for build.
# Excluded here instead of dockerignore so they can be bind mounted during build.
# Saves from copying everything whenever scripts/sql change.
# https://docs.docker.com/reference/dockerfile/#copy---exclude (docker/dockerfile:1.7-labs)
COPY --exclude=.git --exclude=losmeshes/** --exclude=navmeshes/** --exclude=scripts --exclude=sql . /server

ARG CMAKE_BUILD_TYPE=Release
ARG ENABLE_TRACY=OFF
ARG WARNINGS_AS_ERRORS=TRUE

ENV CCACHE_DIR=/root/.ccache
RUN --mount=type=cache,target=/root/build,id=build-ubuntu-$COMPILER \
    --mount=type=cache,target=/root/.ccache,id=ccache-ubuntu-$COMPILER \
    --mount=type=bind,source=.git,target=/server/.git \
    --mount=type=bind,source=scripts,target=/server/scripts \
    --mount=type=bind,source=sql,target=/server/sql <<EOF
set -eux
cp -p /root/build/version.cpp /server/src/common/ 2> /dev/null || true
cp -p /root/build/xi_* /server/ 2> /dev/null || true

if [[ $COMPILER == clang* ]]; then
    export CC=/usr/bin/clang-18
    export CXX=/usr/bin/clang++-18
    export CXXFLAGS="-stdlib=libc++"
    export LDFLAGS="-fuse-ld=lld -lstdc++"
else
    export CC=/usr/bin/gcc-14
    export CXX=/usr/bin/g++-14
fi

cmake -G Ninja -S /server -B /root/build -DENABLE_TRACY=$ENABLE_TRACY -DWARNINGS_AS_ERRORS=$WARNINGS_AS_ERRORS
cmake --build /root/build -j$(nproc)

cp -p /server/xi_* /root/build/
cp -p /server/src/common/version.cpp /root/build/

if [ "$ENABLE_TRACY" = "ON" ]; then
    mv xi_map_tracy xi_map
fi
EOF

###########
# Service #
###########
FROM base AS service

RUN rm -rf /var/lib/apt/lists/*

USER $UNAME

COPY --chown=$UNAME:$UGROUP res/compress.dat res/decompress.dat /server/res/
COPY --chown=$UNAME:$UGROUP scripts /server/scripts
COPY --chown=$UNAME:$UGROUP sql /server/sql
COPY --chown=$UNAME:$UGROUP tools /server/tools
COPY --chown=$UNAME:$UGROUP modules /server/modules
COPY --chown=$UNAME:$UGROUP settings /server/settings

COPY --chown=$UNAME:$UGROUP --from=staging /xiadmin/.venv /xiadmin/.venv
COPY --chown=$UNAME:$UGROUP --from=build /server/xi_* /server/

ARG REPO_URL
ARG BRANCH
RUN <<EOF
if [ -n "$REPO_URL" ] && [ -n "$BRANCH" ]; then
    git clone --bare --filter=tree:0 --branch $BRANCH $REPO_URL /server/.git
    cd /server/.git && git config --local --bool core.bare false
fi
EOF

COPY --chmod=0755 entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/bin/bash"]
