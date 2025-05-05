# syntax=docker/dockerfile:1-labs

########
# Base #
########
FROM alpine:latest AS base

# Install runtime dependencies.
RUN apk --update-cache add \
    binutils \
    git \
    luajit \
    mariadb-client \
    mariadb-connector-c \
    openssl \
    python3 \
    tini \
    tzdata \
    zeromq \
    zlib

RUN git config --system --add safe.directory /server
ENV PATH=/xiadmin/.local/bin:$PATH

ARG UNAME=xiadmin
ARG UGROUP=xiadmin
ARG UID=1000
ARG GID=1000

RUN <<EOF
addgroup --gid $GID $UGROUP
adduser  --uid $UID $UNAME --ingroup $UGROUP --home /xiadmin --disabled-password
EOF

###########
# Staging #
###########
FROM base AS staging

# Install build dependencies.
RUN --mount=type=cache,target=/var/cache/apk,id=var-cache-apk,sharing=locked \
    apk --update-cache add \
    binutils-dev \
    ccache \
    clang18 \
    cmake \
    g++ \
    linux-headers \
    luajit-dev \
    make \
    mariadb-dev \
    openssl-dev \
    python3-dev \
    py3-pip \
    samurai \
    zeromq-dev \
    zlib-dev

USER $UNAME
WORKDIR /server

# Install Python dependencies.
RUN --mount=type=bind,source=./tools/requirements.txt,target=/tmp/requirements.txt \
    --mount=type=cache,target=/xiadmin/.cache/pip,id=cache-pip-alpine,uid=$UID,gid=$GID \
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
    export CC=/usr/bin/gcc
    export CXX=/usr/bin/g++
fi
EOF

ENV CCACHE_DIR=/xiadmin/.ccache
RUN --mount=type=cache,target=/xiadmin/build,id=build-alpine-$COMPILER,uid=$UID,gid=$GID \
    --mount=type=cache,target=/xiadmin/.ccache,id=ccache-alpine-$COMPILER,uid=$UID,gid=$GID \
    --mount=type=bind,source=./.git,target=/server/.git \
    --mount=type=bind,source=./scripts,target=/server/scripts \
    --mount=type=bind,source=./sql,target=/server/sql \
    # --- CACHE ---
    cp -p /xiadmin/build/version.cpp /server/src/common/; \
    cp -p /xiadmin/build/xi_* /server/; \
    # --- End ---
    cmake -G Ninja -S /server -B /xiadmin/build && \
    # --- PATCH efsw ---
    EFSW_FILE="/xiadmin/build/_deps/efsw-src/src/efsw/FileWatcherInotify.cpp" && \
    if [ -f $EFSW_FILE ]; then \
        # Check if include is missing.
        if ! grep -qF '#include <sys/select.h>' $EFSW_FILE; then \
            echo "Patching $EFSW_FILE: Adding #include <sys/select.h>"; \
            sed -i '1i #include <sys/select.h>' $EFSW_FILE; \
        fi; \
        # Check if replacement is needed.
        if grep -qF 'u_int32_t' $EFSW_FILE; then \
            echo "Patching $EFSW_FILE: Replacing u_int32_t with uint32_t"; \
            sed -i 's/u_int32_t/uint32_t/g' $EFSW_FILE; \
        fi; \
    else \
        echo "Warning: $EFSW_FILE not found, skipping patch."; \
    fi; \
    # --- End Patch ---
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
CMD ["/bin/ash"]