# syntax=docker/dockerfile:1-labs

########
# Base #
########
FROM alpine:edge AS base

# Install runtime dependencies.
RUN apk --update-cache add \
    bash \
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

# Setup runtime user.
ARG UNAME=xiadmin
ARG UGROUP=xiadmin
ARG UID=1000
ARG GID=1000
RUN addgroup --gid $GID $UGROUP && \
    adduser  --uid $UID $UNAME --ingroup $UGROUP --home /xiadmin --disabled-password

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
RUN --mount=type=cache,target=/var/cache/apk,id=cache-apk,sharing=locked \
    apk --update-cache add \
    binutils-dev \
    ccache \
    cmake \
    g++ \
    linux-headers \
    luajit-dev \
    make \
    mariadb-dev \
    openssl-dev \
    python3-dev \
    samurai \
    zeromq-dev \
    zlib-dev

# Install Python dependencies.
ENV PATH=/root/.local/bin:$PATH
RUN --mount=type=bind,source=tools/requirements.txt,target=/tmp/requirements.txt \
    --mount=type=cache,target=/root/.cache/pip,id=cache-pip-alpine \
    python3 -m venv $VIRTUAL_ENV && \
    pip3 install --requirement /tmp/requirements.txt

############
# devtools #
############
FROM staging AS devtools

# Install misc dev/ci tools on top of build tools.
RUN apk --update-cache add \
    clang-extra-tools \
    cppcheck \
    gcc \
    lua5.1-dev \
    luarocks \
    && apk cache clean

RUN luarocks-5.1 install luacheck --tree /root/.luarocks
RUN echo 'alias luarocks=luarocks-5.1' >> /etc/profile.d/env.sh

COPY --chmod=0755 entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/bin/bash"]

#########
# Build #
#########
FROM staging AS build

ARG COMPILER=clang20
RUN <<EOF
if [[ $COMPILER == clang* ]]; then
    apk --update-cache add \
    clang \
    compiler-rt \
    libc++-dev \
    lld \
    llvm-libunwind-dev \
    llvm20 \
    && apk cache clean
fi
EOF

# Exclude changes to git metadata, scripts, and sql not needed for build.
# Excluded here instead of dockerignore so they can be bind mounted during build.
# Saves from copying everything whenever scripts/sql change.
# https://docs.docker.com/reference/dockerfile/#copy---exclude (docker/dockerfile:1.7-labs)
COPY --exclude=.git --exclude=losmeshes/** --exclude=navmeshes/** --exclude=scripts --exclude=sql . /server

ARG CMAKE_BUILD_TYPE=Release
ARG ENABLE_TRACY=ON
ARG WARNINGS_AS_ERRORS=FALSE

ENV CCACHE_DIR=/root/.ccache
RUN --mount=type=cache,target=/root/build,id=build-alpine-$COMPILER \
    --mount=type=cache,target=/root/.ccache,id=ccache-alpine-$COMPILER \
    --mount=type=bind,source=.git,target=/server/.git \
    --mount=type=bind,source=scripts,target=/server/scripts \
    --mount=type=bind,source=sql,target=/server/sql <<EOF
set -eux
cp -p /root/build/version.cpp /server/src/common/ 2> /dev/null || true
cp -p /root/build/xi_* /server/ 2> /dev/null || true

if [[ $COMPILER == clang* ]]; then
    export CC=/usr/bin/clang
    export CXX=/usr/bin/clang++
    export CXXFLAGS="-stdlib=libc++ -flto=thin"
    export LDFLAGS="-fuse-ld=lld -flto=thin -lstdc++"
else
    export CC=/usr/bin/gcc
    export CXX=/usr/bin/g++
fi

cmake -G Ninja -S /server -B /root/build -DENABLE_TRACY=$ENABLE_TRACY -DWARNINGS_AS_ERRORS=$WARNINGS_AS_ERRORS

EFSW_FILE="/root/build/_deps/efsw-src/src/efsw/FileWatcherInotify.cpp"
if [ -f $EFSW_FILE ]; then
    if ! grep -qF '#include <sys/select.h>' $EFSW_FILE; then
        echo "Patching $EFSW_FILE: Adding #include <sys/select.h>"
        sed -i '1i #include <sys/select.h>' $EFSW_FILE
    fi
    if grep -qF 'u_int32_t' $EFSW_FILE; then
        echo "Patching $EFSW_FILE: Replacing u_int32_t with uint32_t"
        sed -i 's/u_int32_t/uint32_t/g' $EFSW_FILE
    fi
else
    echo "Warning: $EFSW_FILE not found, skipping patch."
fi

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

RUN apk cache clean

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
