ARG DIST=18

FROM romange/boost-builder:${DIST} as third_party
ARG DIST

# To allow caching third_party libs via "minimal" build.
# We do not use ARG here, not even reference it here because otherwise it would cause cache misses
# during this STEP for different ARGs.
COPY ./CMakeLists.txt /src/
COPY ./cmake /src/cmake
WORKDIR /build
RUN echo "PATH IS: $PATH"
RUN echo "DIST ${DIST}" && cmake -L -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++ \
                                    -DONLY_THIRD_PARTY=ON -GNinja /src
RUN ninja -j4 protobuf_project glog_project sparsehash_project gperf_project zstd_project \
    evhtp_project lz4_project xxhash_project

FROM third_party as src
COPY ./ /src/
RUN mkdir /pkg && cmake -L -DONLY_THIRD_PARTY=OFF -GNinja /src

FROM src as bin
# Now really building the target. Previous containers will be reused between the builds.
ARG TARGET
ARG DIST
RUN echo "Building ${TARGET}" && ninja -j4 $TARGET && strip $TARGET && mv $TARGET /pkg/
RUN ldd -r /pkg/$TARGET | grep "/build/third_party" | awk '{print $3}' | \
    while read n; do cp -nH $n /pkg/; done

FROM romange/boost-prod:${DIST}
ARG TARGET
COPY --from=bin /pkg/ /app/
RUN ldconfig /app && ln -s $TARGET start_app
ENV PATH /app:$PATH

ENTRYPOINT ["start_app"]
